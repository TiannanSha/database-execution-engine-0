package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import org.apache.calcite.rel.RelCollation

import scala.math.Ordered.orderingToOrdered

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var sorted = List[Tuple]()

  // define ordering for tuple
  object TupleOrdering extends Ordering[Tuple] {
    def compare(t1:Tuple, t2:Tuple): Int = {
      //var keysIter = collation.getKeys.iterator()
      var result = 0

      // e.g. field collation = [0 DESC, 2, 1, 3]
      val fieldCollIter = collation.getFieldCollations.iterator()
      while (fieldCollIter.hasNext) {
        var fieldColl = fieldCollIter.next()
        var fieldIndex = fieldColl.getFieldIndex
        if (fieldColl.direction.isDescending) {
          result = t1(fieldIndex).asInstanceOf[Comparable[Elem]] compare
            t2(fieldIndex).asInstanceOf[Comparable[Elem]]
        } else {
          result = t2(fieldIndex).asInstanceOf[Comparable[Elem]] compare
            t1(fieldIndex).asInstanceOf[Comparable[Elem]]
        }
        if (result!=0) {
          // we have decided a winner
          return result
        }
      }
      // all keys are the same, no winner
      return 0
    }
  }

  //val pq = collection.mutable.PriorityQueue[Tuple]()(Ordering.by(t=>t(1).asInstanceOf[Comparable[Elem]]))
  var outputTuples = IndexedSeq[Tuple]()
  var nextOutInd = 0
  var nDrop = 0
  var nTop = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    val fieldCollIter = collation.getFieldCollations.iterator()
    while (fieldCollIter.hasNext) {
      var fieldColl = fieldCollIter.next()
    }
    val fieldColl0 = collation.getFieldCollations.get(0)


    //init variables
    var pq = collection.mutable.PriorityQueue[Tuple]()(TupleOrdering)
    nextOutInd = 0
    nDrop = 0
    nTop = 0

    // insert input tuples into the priority queue
    var inputIter = input.iterator
    var count = 0
    while(inputIter.hasNext) {
      count += 1
      var nextInput = inputIter.next()
      pq.enqueue(nextInput)
    }

    // select top *nTop* tuples and then drop *nDrop* tuples
    if (offset.isEmpty) {
      nDrop = 0
    } else {
      nDrop = offset.get
    }

    if (!fetch.isEmpty) {
      // when fetch is specified, take the min(pq.size, fetch) number of tuples
      nTop = nDrop + Math.min(fetch.get, pq.size)
    } else {
      // when not specifying fetch, want all available tuples
      nTop = pq.size
    }
    println(s"nTop = $nTop")
//    outputTuples = IndexedSeq(1 to nTop).map(_=>pq.dequeue)
    for (i <- 1 to nTop) {
      outputTuples = outputTuples :+ pq.dequeue()
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (nextOutInd < outputTuples.length) {
      var nextOut = outputTuples(nextOutInd)
      nextOutInd += 1
      return Option(nextOut)
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
