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
      AlarmasIn(arr(0).toInt, arr(1), arr(2), arr(3))
    })


    parsed
    .keyBy(_.country) // key by product id.
      .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
      .process(new ProcessWindowFunction[
        AlarmasIn, AlarmasOut, String, TimeWindow
      ]() {
        override def process(key: String, context: Context,
                             elements: Iterable[AlarmasIn],
                             out: Collector[AlarmasOut]): Unit = {
          val lst = elements.toList
          lst.foreach(x => out.collect(AlarmasOut(x.id, x.country, x.city,x.address, lst.size,0,0)))
      }
      })
      .keyBy( _.city).window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
        .process(new ProcessWindowFunction[
          AlarmasOut, AlarmasOut, String, TimeWindow
        ]() {
          override def process(key: String,
                               context: Context,
                               elements: Iterable[AlarmasOut],
                               out: Collector[AlarmasOut]): Unit = {
            val lst = elements.toList
            lst.foreach(x => out.collect(AlarmasOut(x.id, x.country, x.city,x.address,x.c_country,lst.size,x.c_addr)))
          }
        })
      .keyBy( _.address).window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .process(new ProcessWindowFunction[
        AlarmasOut, AlarmasOut, String, TimeWindow
      ]() {
        override def process(key: String,
                             context: Context,
                             elements: Iterable[AlarmasOut],
                             out: Collector[AlarmasOut]): Unit = {
          val lst = elements.toList
          lst.foreach(x => out.collect(AlarmasOut(x.id, x.country, x.city,x.address,x.c_country,x.c_city,lst.size)))
        }
      })
      .print()



    senv.execute("kafka Window WordCount")

  }


  // *************************************************************************
  //      DATA TYPES
  // *************************************************************************

  case class AlarmasIn(
                      id: Int,
                      country: String,
                      city: String,
                      address: String
                    )

  case class AlarmasOut(
                       id: Int,
                       country: String,
                       city: String,
                       address: String,
                       c_country: Int,
                       c_city: Int,
                       c_addr: Int
                     )


}
