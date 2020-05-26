package com.learn

import com.learn.utils.ESSinkUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost

object FlinkSink2EsScala {
  def main(args: Array[String]): Unit = {

    val httpHosts: java.util.ArrayList[HttpHost] = new java.util.ArrayList()
    httpHosts.add(new HttpHost("10.31.53.185", 9200, "http"))
    val esUperstSink: ElasticsearchSink.Builder[String] = ESSinkUtils.getUpsertJsonStrESSinkWithDefaultParameters(httpHosts)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = env.readTextFile("D:\\bdproject\\flinkSink2Es\\src\\main\\resources\\data.txt")
    env.setParallelism(1)
    data.addSink(esUperstSink.build()).setParallelism(1).disableChaining()

    env.execute("demo")


  }

}
