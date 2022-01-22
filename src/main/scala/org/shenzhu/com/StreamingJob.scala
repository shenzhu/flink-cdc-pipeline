/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shenzhu.com

import com.ververica.cdc.connectors.postgres.PostgreSQLSource
import com.ververica.cdc.debezium.{
  DebeziumSourceFunction,
  JsonDebeziumDeserializationSchema
}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

object StreamingJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    val sourceFunction: DebeziumSourceFunction[String] =
      PostgreSQLSource
        .builder[String]()
        .hostname("localhost")
        .port(5432)
        .database("postgres")
        .schemaList("public")
        .tableList("public.shipments")
        .username("postgres")
        .password("postgres")
        .deserializer(new JsonDebeziumDeserializationSchema())
        .build()

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
      new Configuration()
    )
    env.setParallelism(1)

    val dataStream: DataStream[String] = env.addSource(sourceFunction)
    dataStream.map(new CDCRecordMap).addSink(new ClickHouseSink)

    // execute program
    env.execute("Flink CDC Pipeline")
  }
}
