package com.learn.utils

import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType

/**
  * author: lian.duan
  * time：2019.11.27
  * description:
  * user-defined elasticsearch request
  */

object ESClientFunction extends Serializable {

  import org.slf4j.LoggerFactory

  val log = LoggerFactory.getLogger(this.getClass)

  /**
    * create insert es request
    *
    * @param index
    * @param esType
    * @param id
    * @param jsonMap
    * @return
    */
  def createIndexRequest(index: String, esType: String, id: String, jsonMap: java.util.HashMap[String, Object]): IndexRequest = {
    Requests.indexRequest()
      .index(index)
      .`type`(esType)
      .id(id)
      .source(jsonMap)
  }

  /**
    * create es getRequest
    *
    * @param index
    * @param esType
    * @param id
    * @return
    */
  def createGetRequest(index: String, esType: String, id: String): GetRequest = {
    Requests.getRequest(index)
      .`type`(index)
      .id(id)
  }

  /**
    * create es deleteRequest
    *
    * @param index
    * @param esType
    * @param id
    * @return
    */
  def createDeleteRequest(index: String, esType: String, id: String): DeleteRequest = {
    Requests.deleteRequest(index)
      .`type`(esType)
      .id(id)
  }

  /**
    * create es updateRequest
    *
    * @param index
    * @param esType
    * @param id
    * @param jsonMap
    * @return
    */
  def createUpdateRequest(index: String, esType: String, id: String, jsonMap: java.util.HashMap[String, Object]): UpdateRequest = {
    new UpdateRequest()
      .index(index)
      .`type`(esType)
      .id(id)
      .doc(jsonMap)
  }

  /**
    * create es updateRequest
    *
    * @param index
    * @param esType
    * @param id
    * @param jsonStr
    * @return
    */
  def createUpdateRequest(index: String, esType: String, id: String, jsonStr: String): UpdateRequest = {
    new UpdateRequest()
      .index(index)
      .`type`(esType)
      .id(id)
      .doc(jsonStr, XContentType.JSON)
  }

  /**
    * create es upsertRequest
    *
    * @param index
    * @param esType
    * @param id
    * @param jsonMap
    * @return
    */
  def createUpsertRequest(index: String, esType: String, id: String, jsonMap: java.util.HashMap[String, Object]): UpdateRequest = {
    //    log.info("Elasticsearch-index >>> " + (index, esType, id))
    createUpdateRequest(index, esType, id, jsonMap).upsert(jsonMap)
  }

  def createUpsertRequest(index: String, esType: String, id: String, jsonStr: String): UpdateRequest = {
    createUpdateRequest(index, esType, id, jsonStr).
    upsert(jsonStr, XContentType.JSON)
  }

}
