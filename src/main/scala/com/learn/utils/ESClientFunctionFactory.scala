package com.learn.utils

import java.util

import com.alibaba.fastjson.JSON
import com.learn.constant.ESConfigType
import com.learn.utils.ESParseUtil.ESIndexConfig
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}

object ESClientFunctionFactory extends Serializable {

  def getESUpsertJsonStrFunctionInstance(): ElasticsearchSinkFunction[String] = {
    new ElasticsearchSinkFunction[String]() {
      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        val jsonMap: util.HashMap[String, Object] = JSON.parseObject(element, classOf[java.util.HashMap[String, Object]])
        val esConfigMap = JSON.parseObject(jsonMap.get(ESConfigType.ESCONFIG).toString,classOf[java.util.HashMap[String,Object]])
        val ESIndexConfig(index, esType, id) = ESParseUtil.parseESIndexConfig(esConfigMap)
        indexer.add(ESClientFunction.createUpsertRequest(index, esType, id, element))
      }
    }
  }

}
