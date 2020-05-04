package com.flink.demo

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object BatchDemo {
  case class WordWithCount(word: String, count: Long)


  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val path = "textodemo.txt"
    val text = env.readTextFile(path,"UTF-8")

    val counts = text
      .flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1)}
      .groupBy(0)
      .sum(1)
      .sortPartition(1,Order.DESCENDING)

    counts.print()


    println("fin")

  }

}
