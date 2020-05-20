package com.flink.datatable

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector


object WindowFunctionAgg {


  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "0")

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream2 = senv
      .addSource(new FlinkKafkaConsumer[String]("flink", new SimpleStringSchema(), properties))


    val parsed = stream2.map(x=> {
      val arr = x.split(",")
      (arr(0).toInt, arr(1), arr(2).toInt)
    })


    parsed
    .keyBy(x => x._2) // key by product id.
      .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
      .process(new ProcessWindowFunction[
        (Int, String, Int), (Int, String, Int, Int), String, TimeWindow   //, String, Int, Int
      ]() {
        override def process(key: String, context: Context,
                             elements: Iterable[(Int, String, Int)],
                             out: Collector[(Int, String, Int, Int)]): Unit = {  //, String, Int, Int
          val lst = elements.toList
          lst.foreach(x => out.collect((x._1, x._2, x._3, lst.size)))


      }
      }).print()




    //windowCounts.print()

    senv.execute("kafka Window WordCount")

  }

  // *************************************************************************
  //      DATA TYPES
  // *************************************************************************

  case class Order(user: Int, product: String, amount: Int)

}
