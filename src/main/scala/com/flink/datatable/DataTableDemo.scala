package com.flink.datatable

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}


/*
Documentation by Alibaba
https://www.alibabacloud.com/help/doc-detail/62514.htm?spm=a2c63.p38356.b99.45.2edc28e45KFJxS

 */




object DataTableDemo {


  def main(args: Array[String]): Unit = {

    // **********************
    // FLINK STREAMING QUERY
    // **********************
    val params = ParameterTool.fromArgs(args)
    val planner = if (params.has("planner")) params.get("planner") else "blink"
    println(planner+"\n")


    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val tEnv = if (planner == "blink") {  // use blink planner in streaming mode
      val settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build()
      StreamTableEnvironment.create(env, settings)
    } else if (planner == "flink") {  // use flink planner in streaming mode
      val settings = EnvironmentSettings.newInstance()
        .useOldPlanner()
        .inStreamingMode()
        .build()
      StreamTableEnvironment.create(env, settings)
    } else {
      System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
        "where planner (it is either flink or blink, and the default is flink) indicates whether the " +
        "example uses flink planner or blink planner.")
      return
    }

    // -----------------------------------------------------

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "0")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink", new SimpleStringSchema(), properties) )
    // ---------------------------------------

    val parsed: DataStream[Order] = stream.map(x=> {
      val arr = x.split(",")
      Order(arr(0).toInt, arr(1), arr(2).toInt)
    })

    val ds: DataStream[Order] = env.fromCollection(Seq(
      Order(1, "bread", 30),
      Order(2, "bread", 30),
      Order(3, "bread", 10),
      Order(1, "milk", 30),
      Order(2, "milk", 30),
      Order(4, "bear", 10)
    ))


    // register DataStream as Table
    val tableA = tEnv.fromDataStream(ds, 'user, 'product, 'amount)

    tEnv.registerDataStream("OrdersB", parsed, 'user, 'product, 'amount, 'proctime.proctime)

    // union the two tables
    val result = tEnv.sqlQuery(
      s"""
        |select * from (
        |SELECT *,
        |     COUNT(user) OVER (PARTITION BY product) as c_num
        |FROM $tableA
        |)
        |""".stripMargin)

    result.toAppendStream[(Int, String, Int,Long)].print()



    val result2 = tEnv.sqlQuery(
      """
        |SELECT product, TUMBLE_END(proctime, INTERVAL '5' SECOND) AS t, SUM(amount)
        |FROM OrdersB
        |GROUP BY TUMBLE(proctime, INTERVAL '5' SECOND), product
        |""".stripMargin)

    val result3 = tEnv.sqlQuery(
      """
        |SELECT
        |	user, product, amount, amountCnt
        |FROM (
        |	SELECT
        |		user,
        |   product,
        |   amount,
        |		COUNT(user) OVER w AS amountCnt
        |   FROM(SELECT * FROM OrdersB)
        |    WINDOW w AS (
        |		      partition by product
        |        ORDER BY proctime RANGE BETWEEN INTERVAL '5' SECOND PRECEDING AND CURRENT ROW
        |    )
        |)
        |""".stripMargin)

    val result4 = tEnv.sqlQuery(
      """
        |SELECT *, COUNT(user) OVER (PARTITION BY product ORDER BY wStart ASC) as row_num
        |FROM(
        |   SELECT user, product, amount,
        |     TUMBLE_START(proctime, INTERVAL '30' SECOND) as wStart,
        |     COUNT(user)  as cont
        |   FROM OrdersB
        |   GROUP BY TUMBLE(proctime, INTERVAL '30' SECOND), user, product, amount
        |
        |  )
        |""".stripMargin)

    //result.toRetractStream[(Int, String, Long)].print()
    //result2.toRetractStream[(Int, Int)].print()
//    result2.toRetractStream[(String,Timestamp, Int)].print()

    //result4.toRetractStream[(Int, String, Int, Timestamp,Long ,Long)].print()





    env.execute("kafka Window")

  }

  // *************************************************************************
  //      DATA TYPES
  // *************************************************************************

  case class Order(user: Int, product: String, amount: Int)

}
