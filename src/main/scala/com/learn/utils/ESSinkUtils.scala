package com.learn.utils

import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost

object ESSinkUtils extends Serializable{

  def getUpsertJsonStrESSinkWithDefaultParameters(httpHosts: java.util.ArrayList[HttpHost]): ElasticsearchSink.Builder[String] = {
    val esSinkBuilder = ESSinkUtils.getJsonStrUpsertESSink(httpHosts)
    /*     必须设置flush参数     */
    //刷新前缓冲的最大动作量
    esSinkBuilder.setBulkFlushMaxActions(10000)
    //刷新前缓冲区的最大数据大小（以MB为单位）
    esSinkBuilder.setBulkFlushMaxSizeMb(100)
    //论缓冲操作的数量或大小如何都要刷新的时间间隔
    esSinkBuilder.setBulkFlushInterval(60 * 1000)
    //添加失败重试
    esSinkBuilder.setFailureHandler(new NoThrowExceptionActionFailingHandler)
    esSinkBuilder
  }


  def getJsonStrUpsertESSink(httpHosts: java.util.ArrayList[HttpHost]): ElasticsearchSink.Builder[String] = {
    new ElasticsearchSink.Builder(
      httpHosts,
      ESClientFunctionFactory.getESUpsertJsonStrFunctionInstance()
    )
  }




}
