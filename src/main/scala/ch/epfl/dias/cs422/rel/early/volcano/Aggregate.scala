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
  var inputTuplesGrouped = Map[Tuple, IndexedSeq[Tuple]]()
  var nextTupleInd : Int = 0
  var allAggedTuples = IndexedSeq[Tuple]() // each tuple is the result from aggregating a group

  // get the key (which is a smaller tuple) of a tuple
  def getKey(t: Tuple): Tuple = {
    var key = IndexedSeq()
    //var groupList = groupSet
    var groupSetIter = groupSet.iterator()
    while (groupSetIter.hasNext){
      key :+ t(groupSetIter.next())
    }
    return key
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    // init variables
    nextTupleInd = 0

    // read each tuple into appropriate group
    var inputIter = input.iterator
    while(inputIter.hasNext) {
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

    // if all groups are empty, return default value for each agg
    if (inputTuplesGrouped.isEmpty) {
      var aggedTuple = IndexedSeq[Any]()
      for (agg <- aggCalls) {
        aggedTuple = aggedTuple :+ agg.emptyValue
      }
      allAggedTuples = allAggedTuples :+ aggedTuple
      return
    }

    // aggregate each group if there are some non-empty groups
    for (group <- inputTuplesGrouped.values) {
      var aggedTuple = IndexedSeq[Any]()
      // aggregate group
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
  override def close(): Unit = ???
}
