package com.learn.utils

import com.learn.base.BaseLogger
import lombok.extern.slf4j.Slf4j
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, RequestIndexer}
import org.apache.flink.util.ExceptionUtils
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.slf4j.LoggerFactory

class NoThrowExceptionActionFailingHandler extends ActionRequestFailureHandler
  with Serializable with BaseLogger {


  val config: Map[String, String] = Map()
  //1、用来表示是否开启重试机制
  config.+("bulk.flush.backoff.enable" -> "true")
  //2、重试策略，又可以分为以下两种类型
  //a、指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
  //  config.+("bulk.flush.backoff.type" -> "EXPONENTIAL")
  //b、常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...
  config.+("bulk.flush.backoff.type" -> "2")
  //3、进行重试的时间间隔。对于指数型则表示起始的基数
  config.+("bulk.flush.backoff.delay" -> "2")
  //4、失败重试的次数
  config.+("bulk.flush.backoff.retries" -> "3")

  private val serialVersionUID = -1111111111111111111L

  @throws[Throwable]
  override def onFailure(action: ActionRequest, failure: Throwable,
                         restStatusCode: Int, indexer: RequestIndexer): Unit = {
    if (ExceptionUtils.findThrowable(failure, classOf[EsRejectedExecutionException]).isPresent)
      indexer.add(action)
    else // rethrow all cell failures
      log.error("[ FLINK--> ELASTICSEARCH :" + failure.getMessage + "]\r\n" + action.toString)
  }
}
