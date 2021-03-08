package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple =
    eval(projects.asScala.toIndexedSeq, input.getRowType)

  var inputTuples : IndexedSeq[Tuple] = IndexedSeq()
  var nextTupleInd : Int = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    // init state variables
    inputTuples = IndexedSeq[Tuple]()
    nextTupleInd = 0

    // read in all input tuples
    var inputIter = input.iterator
    while(inputIter.hasNext){
      inputTuples = inputTuples :+ inputIter.next()
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (nextTupleInd < inputTuples.length){
      var nextTuple = inputTuples(nextTupleInd)
      nextTupleInd += 1
      // apply project function to get projected new tuple
      var nextTupleProjected = evaluator(nextTuple)
      return Option(nextTupleProjected)
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
