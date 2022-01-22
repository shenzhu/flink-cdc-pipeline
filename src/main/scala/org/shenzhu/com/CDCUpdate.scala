package org.shenzhu.com

import scala.collection.mutable.ListBuffer

case class CDCUpdate(
    operation: String,
    fieldList: ListBuffer[String],
    afterValueList: Option[ListBuffer[String]],
    beforeValueList: Option[ListBuffer[String]]
)
