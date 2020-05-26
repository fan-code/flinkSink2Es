package com.learn.base

/**
  * global  logger
  */
trait BaseLogger {

  import org.slf4j.LoggerFactory

  val log = LoggerFactory.getLogger(this.getClass)

}
