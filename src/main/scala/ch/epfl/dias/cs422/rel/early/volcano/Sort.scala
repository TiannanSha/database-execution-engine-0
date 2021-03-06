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
//  val keysIter = collation.getKeys.iterator()
//  def tupleToKeys(t:Tuple):Tuple= {
//    var key = IndexedSeq[Any]()
//    while (keysIter.hasNext) {
//      val k = keysIter.next()
//      key = key :+ t(k).asInstanceOf[Comparable[Elem]]
//    }
//    return key
//  }
//  val orderingByKeys: Ordering[Tuple] = Ordering.by(t => tupleToKeys(t))


  // define ordering for tuple
  object TupleOrdering extends Ordering[Tuple] {
    def compare(t1:Tuple, t2:Tuple): Int = {
      var keysIter = collation.getKeys.iterator()
      var result = 0
      while (keysIter.hasNext) {
        // compare collation keys one by one
        // until a key differentiate the two tuples or all keys are compared
        var k = keysIter.next()
        //result = t1(k).asInstanceOf[Comparable[Elem]] compare t2(k).asInstanceOf[Comparable[Elem]]
        result = t2(k).asInstanceOf[Comparable[Elem]] compare t1(k).asInstanceOf[Comparable[Elem]]
        if (result!=0) {
          return result
        }
        // else keep comparing other keys
      }
      // all keys are the same
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
    println()
    println("**** In Sort ****")
//    println("***IN Open")
//    println(s"collation = $collation")
//    val fieldColl = collation.getFieldCollations()
//    val keys = collation.getKeys()
//    println(s"fieldColls = $fieldColl")
//    println(s"keys = $keys")
//    println(s"offset = $offset")
//    println(s"fetch = $fetch")

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
    println(s"inputCount = $count")
    println("****pq.size:****")
    println(pq.size)
    nTop = pq.size

    // select top *nTop* tuples and then drop *nDrop* tuples
    if (offset.isEmpty) {
      nDrop = 0
    } else {
      nDrop = offset.get
    }

    if (!fetch.isEmpty) {
      nTop = nDrop + fetch.get
    }
    println(s"nTop = $nTop")
//    outputTuples = IndexedSeq(1 to nTop).map(_=>pq.dequeue)
    for (i <- 1 to nTop) {
      outputTuples = outputTuples :+ pq.dequeue()
    }
    println("***outputTuples***")
    println(outputTuples)

    println("**** Exit Sort ****")
    println()
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
  override def close(): Unit = ???
}
