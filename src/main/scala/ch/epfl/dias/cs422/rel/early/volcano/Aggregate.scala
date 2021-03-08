package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  // group variable

  // state variables
  //var inputTuplesGrouped = Map[Tuple, IndexedSeq[Tuple]]()
  var nextTupleInd : Int = 0
  var allAggedTuples = IndexedSeq[Tuple]() // each tuple is the result from aggregating a group

  // get the key (which is a smaller tuple) of a tuple
  def getKey(t: Tuple): IndexedSeq[Any] = {
    var key = IndexedSeq[Any]()
    //var groupList = groupSet
    var groupSetIter = groupSet.iterator()
    while (groupSetIter.hasNext){
      val nextK = groupSetIter.next()
      key = key :+ t(nextK.toInt)
    }
    return key
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {


    // init variables
    allAggedTuples = IndexedSeq[Tuple]()
    var inputTuplesGrouped = Map[IndexedSeq[Any], IndexedSeq[Tuple]]()
    nextTupleInd = 0

    // read each tuple into appropriate group
    var inputIter = input.iterator
    var inputCount = 0
    while(inputIter.hasNext) {
      inputCount+=1
      var nextInput = inputIter.next()
      var k = getKey(nextInput)

      // insert into the dictionary
      // if key already exists, append to the list
      // if key is new, add (key, listOfTuple) to the dictionary
      var groupK = inputTuplesGrouped.get(k)
      if (groupK.isEmpty)
        inputTuplesGrouped += (k->IndexedSeq(nextInput))
      else {
        inputTuplesGrouped += (k->groupK.get.appended(nextInput))
      }
    }

    // if all groups are empty and groupby clause is empty, return empty value for each agg
    if (inputTuplesGrouped.isEmpty && groupSet.isEmpty) {
      //println(s"groupSet.isEmpty = ${groupSet.isEmpty}")
      var aggedTuple:Tuple = IndexedSeq()
      for (agg <- aggCalls) {
        aggedTuple = aggedTuple :+ agg.emptyValue
      }
      allAggedTuples = allAggedTuples :+ aggedTuple
      return
    }

    // aggregate each non-empty groups
    for (key <- inputTuplesGrouped.keys) {
      var group = inputTuplesGrouped(key)
      var aggedTuple:Tuple = IndexedSeq()
      // add keys first
      aggedTuple = aggedTuple ++ key.asInstanceOf[Tuple]
      // for each aggregation, map group to agg args and reduce to get the final aggregated value
      // append all the aggregated values to get final aggregated tuple for a group
      for (agg <- aggCalls) {
        var mappedGroup = group.map(tuple => agg.getArgument(tuple))
        var aggedVal = mappedGroup.reduce(agg.reduce)
        aggedTuple = aggedTuple :+ aggedVal
      }
      allAggedTuples = allAggedTuples :+ aggedTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (nextTupleInd < allAggedTuples.length) {
      var nextTuple = allAggedTuples(nextTupleInd)
      nextTupleInd += 1
      return Option(nextTuple)
    } else {
      return NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {

  }
}
