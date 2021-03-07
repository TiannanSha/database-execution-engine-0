package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getLeftKeys]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getRightKeys]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {


  //val leftHashmap = scala.collection.mutable.HashMap.empty[Tuple, Tuple]
  var allJoinedTuples = IndexedSeq[Tuple]()
  var nextTupleInd = 0

  def getKeyAsTuple(tuple: Tuple, keyIndices:IndexedSeq[Int]): Tuple = {
    var key = IndexedSeq[Any]()
    for (i <- keyIndices) {
      key = key :+ tuple(i)
    }
    return key
  }


  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    // init variables
    var leftHashmap = scala.collection.mutable.HashMap.empty[Tuple, IndexedSeq[Tuple]]
    nextTupleInd = 0
    allJoinedTuples = IndexedSeq[Tuple]()
    var leftTuples = IndexedSeq[Tuple]()

    // store all tuples from left table to the hash table
    // so that they are grouped by the left join key
    var leftIter = left.iterator
    var leftCount = 0
    while (leftIter.hasNext) {
      var nextLeft = leftIter.next()
      var nextLeftKey = getKeyAsTuple(nextLeft, getLeftKeys)

      // if this is a new key, insert a new (key->Seq(nextLeft))
      if (!leftHashmap.contains(nextLeftKey)) {
        leftHashmap += (nextLeftKey -> (IndexedSeq[Tuple]() :+ nextLeft))
        leftCount += 1
        leftTuples = leftTuples :+ nextLeft
      } else {
        // if this is a existing key, append to corresponding group
        leftHashmap += (  nextLeftKey -> (leftHashmap(nextLeftKey) :+ nextLeft)  )
        leftCount += 1
        leftTuples = leftTuples :+ nextLeft
      }

    }

    // read tuple from right table one by one and do the join by
    // concatnating with all tuples in the bucket with same key
    var rightIter = right.iterator
    var rightCount = 0
    var rightTuples = IndexedSeq[Tuple]()
    while (rightIter.hasNext) {
      rightCount += 1
      var nextRight = rightIter.next()
      rightTuples = rightTuples :+ nextRight
      val rightKey = getKeyAsTuple(nextRight, getRightKeys)
      if (leftHashmap.contains(rightKey)) {
        // found matching left key, join with each member in the group
        val matchedLeftTuples = leftHashmap(rightKey)
        for (l <- matchedLeftTuples) {
          var joinedTuple = l ++ nextRight
          allJoinedTuples = allJoinedTuples :+ joinedTuple
        }
      }
//      var leftTuplesMatched = leftHashmap.get(rightKey)
//      if (!leftTuplesMatched.isEmpty) {
//        // only need to add more joined tuples if there are matched left tuples
//        for (l <- leftTuplesMatched) {
//          var joinedTuple = l ++ nextRight
//          allJoinedTuples = allJoinedTuples :+ joinedTuple
//        }
//      }
    }
    println()
    println("****In Join****")
    println(s"getLeftKeys = $getLeftKeys")
    println(s"getRightKeys = $getRightKeys")
    val outputLen = allJoinedTuples.length
    println(s"leftTuples = $leftTuples")
    println(s"leftHashmap = $leftHashmap")
    println("right tuples")
    //println(rightTuples)
    println(s"outputLength = $outputLen")
    println(s"leftCount = $leftCount")
    println(s"rightCount = $rightCount")
    println("****In Join****")
    println()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (nextTupleInd < allJoinedTuples.length) {
      val nextTuple = Option(allJoinedTuples(nextTupleInd))
      nextTupleInd += 1
      return nextTuple
    } else {
      return NilTuple
    }

  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = ???
}
