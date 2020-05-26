package com.learn.utils

import com.learn.constant.ESConfigType

object ESParseUtil extends Serializable {

  case class ESIndexConfig(index: String, esType: String, id: String)

  def parseESIndexConfig(jsonMap: java.util.HashMap[String, Object]): ESIndexConfig = {
    val index = jsonMap.get(ESConfigType.INDEX).asInstanceOf[String]
    val esType = jsonMap.get(ESConfigType.TYPE).asInstanceOf[String]
    val id = jsonMap.get(ESConfigType.ID).asInstanceOf[String]
    ESIndexConfig(index, esType, id)
  }

}
