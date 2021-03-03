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


  val leftHashmap = scala.collection.mutable.HashMap.empty[Tuple, Tuple]
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
    nextTupleInd = 0

    // store all tuples from left table to the hash table
    var leftIter = left.iterator
    while (leftIter.hasNext) {
      var nextLeft = leftIter.next()
      leftHashmap += (getKeyAsTuple(nextLeft, getLeftKeys) -> nextLeft)
    }

    // read tuple from right table one by one and do the join by
    // concatnating with all tuples in the bucket with same key
    var rightIter = right.iterator
    while (rightIter.hasNext) {
      var nextRight = rightIter.next()
      val rightKeys = getKeyAsTuple(nextRight, getRightKeys)
      var leftTuplesMatched = leftHashmap.get(rightKeys)
      if (!leftTuplesMatched.isEmpty) {
        // only need to add more joined tuples if there are matched left tuples
        for (l <- leftTuplesMatched) {
          var joinedTuple = l ++ nextRight
          allJoinedTuples = allJoinedTuples :+ joinedTuple
        }

      }
    }
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
