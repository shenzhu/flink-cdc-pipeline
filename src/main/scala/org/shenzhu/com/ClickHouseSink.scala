package org.shenzhu.com

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}
import sttp.client3.{
  HttpURLConnectionBackend,
  Response,
  UriContext,
  basicRequest
}

import scala.collection.mutable.ListBuffer

class ClickHouseSink extends RichSinkFunction[CDCUpdate] {
  private lazy val backend = HttpURLConnectionBackend()
  private val clickHouseHost: String = "localhost"
  private val clickHousePort: Int = 8123
  private val clickHouseUser: String = "clickhouse"
  private val clickHousePassword: String = "clickhouse"

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def invoke(value: CDCUpdate, context: SinkFunction.Context): Unit = {
    println(value)

    val body: String = value.operation match {
      case "r" | "c" =>
        val fields: String = "(" + value.fieldList.mkString(",") + ")"
        val values: String = "(" + value.afterValueList.get.mkString(",") + ")"
        s"INSERT INTO flinkcdc.shipments${fields} VALUES ${values}"

      case "u" =>
        val fields: ListBuffer[String] = value.fieldList
        val beforeValues: ListBuffer[String] = value.beforeValueList.get
        val afterValues: ListBuffer[String] = value.afterValueList.get

        // Find diff
        val updateOpsBuffer: ListBuffer[String] = new ListBuffer[String]()
        val filterOpsBuffer: ListBuffer[String] = new ListBuffer[String]()
        for (i <- fields.indices) {
          if (beforeValues(i) != afterValues(i)) {
            updateOpsBuffer.append(fields(i) + "=" + afterValues(i))
            filterOpsBuffer.append(fields(i) + "=" + beforeValues(i))
          }
        }
        val updateOps = updateOpsBuffer.mkString(",")
        val filterOps = filterOpsBuffer.mkString(" AND ")

        s"ALTER TABLE flinkcdc.shipments UPDATE ${updateOps} WHERE ${filterOps}"

      case "d" =>
        val fields: ListBuffer[String] = value.fieldList
        val beforeValues: ListBuffer[String] = value.beforeValueList.get

        val filterOpsBuffer: ListBuffer[String] = new ListBuffer[String]()
        for (entry <- fields zip beforeValues) {
          filterOpsBuffer.append(entry._1 + "=" + entry._2)
        }
        val filterOps = filterOpsBuffer.mkString(" AND ")

        s"ALTER TABLE flinkcdc.shipments DELETE WHERE ${filterOps}"
    }

    val request: Request[Either[String, String], Any] = basicRequest
      .body(body)
      .post(
        uri"http://$clickHouseUser:$clickHousePassword@$clickHouseHost:$clickHousePort/"
      )
    val response: Identity[Response[Either[String, String]]] =
      request.send(backend)

    // Error handling logic
    println(response)
  }
}
