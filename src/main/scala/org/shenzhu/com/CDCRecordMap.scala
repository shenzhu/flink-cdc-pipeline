package org.shenzhu.com

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import org.apache.flink.api.common.functions.MapFunction
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class CDCRecordMap extends MapFunction[String, CDCUpdate] {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Process the given field in CDC update JSON
    * @param jsonObj CDC update JSON
    * @param field Field name in update JSON
    * @return
    */
  def processFieldRecord(
      jsonObj: JSONObject,
      field: String
  ): Tuple2[ListBuffer[String], ListBuffer[String]] = {
    val nameList: ListBuffer[String] = new ListBuffer[String]()
    val valueList: ListBuffer[String] = new ListBuffer[String]()

    val value: JSONObject = jsonObj.getJSONObject(field)
    for (fieldName <- value.keySet()) {
      nameList.append(fieldName)
      valueList.append(s"'${value.getString(fieldName)}'")
    }

    Tuple2(nameList, valueList)
  }

  override def map(value: String): CDCUpdate = {
    try {
      val jsonObj: JSONObject = JSON.parseObject(value)
      val op = jsonObj.getString("op")

      op match {
        case "r" =>
          // Take consistent snapshot of all data
          val fieldAndValue =
            processFieldRecord(jsonObj, "after")
          CDCUpdate("r", fieldAndValue._1, Option(fieldAndValue._2), None)

        case "c" =>
          // Insert value
          val fieldAndValue =
            processFieldRecord(jsonObj, "after")
          CDCUpdate("c", fieldAndValue._1, Option(fieldAndValue._2), None)

        case "u" =>
          // Update value
          val beforeFieldAndValue =
            processFieldRecord(jsonObj, "before")
          val afterFieldAndValue = processFieldRecord(jsonObj, "after")
          CDCUpdate(
            "u",
            afterFieldAndValue._1,
            Option(afterFieldAndValue._2),
            Option(beforeFieldAndValue._2)
          )

        case "d" =>
          // Delete value
          val fieldAndValue =
            processFieldRecord(jsonObj, "before")
          CDCUpdate("d", fieldAndValue._1, None, Option(fieldAndValue._2))
      }
    } catch {
      case e: JSONException =>
        logger.error(s"Failed to parse text as json: ${value}")
        null
    }
  }
}
