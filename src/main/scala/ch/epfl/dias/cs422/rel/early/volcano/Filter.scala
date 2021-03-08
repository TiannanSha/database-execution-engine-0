package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  // state variables
  var inputTuples = IndexedSeq[Tuple]()
  var nextTupleInd : Int = 0
  var outputCount = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // init variables
    inputTuples = IndexedSeq[Tuple]()
    nextTupleInd = 0
    outputCount = 0

    // read in all input tuples
    var inputIter = input.iterator
    var inputCount = 0
    while(inputIter.hasNext) {
      inputTuples = inputTuples :+ inputIter.next()
      inputCount += 1
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    //println(s"outputCount = $outputCount")
    while (nextTupleInd < inputTuples.length){
      var nextTuple = inputTuples(nextTupleInd)
      nextTupleInd += 1
      // keep searching for next tuple that should pass the filter
      if (predicate(nextTuple)) {
        outputCount += 1
        return Option(nextTuple)
      }
    }
    // haven't found a tuple that satisfy the predicate
    return NilTuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {

  }
}
