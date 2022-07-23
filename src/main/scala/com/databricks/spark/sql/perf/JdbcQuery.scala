/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.JdbcQuery.getConnection

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.SparkPlan

import java.sql.{Connection, DriverManager}


/** Holds one benchmark query and its metadata. */
class JdbcQuery(
             override val name: String,
             url: String,
             val sqlText: String,
             val description: String = "",
             override val executionMode: ExecutionMode = ExecutionMode.ForeachResults)
  extends Benchmarkable with Serializable {

  private implicit def toOption[A](a: A): Option[A] = Option(a)

  override def toString: String = s"== Query: $name =="

  protected override def doBenchmark(
                                      includeBreakdown: Boolean,
                                      description: String = "",
                                      messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      val breakdownResults = Seq.empty[BreakdownResult]

      // The executionTime for the entire query includes the time of type conversion from catalyst
      // to scala.
      // Note: queryExecution.{logical, analyzed, optimizedPlan, executedPlan} has been already
      // lazily evaluated above, so below we will count only execution time.
      val executionTime = measureTimeMs {
        val conn = getConnection(url)
        val stmt = conn.createStatement()
        stmt.executeQuery(sqlText)
        stmt.close()
      }

      BenchmarkResult(
        name = name,
        mode = executionMode.toString,
        executionTime = executionTime,
        breakDown = breakdownResults)
    } catch {
      case e: Exception =>
        BenchmarkResult(
          name = name,
          mode = executionMode.toString,
          failure = Failure(e.getClass.getName, e.getMessage))
    }
  }
}

case object JdbcQuery {
  private val connMap = new mutable.HashMap[String, Connection]

  private def getConnection(url: String): Connection = {
    if (!connMap.contains(url)) {
      connMap(url) = DriverManager.getConnection(url)
    }
    connMap(url)
  }

  def apply(url: String, query: Query): Option[JdbcQuery] = {
    query.sqlText.map(sqlText => new JdbcQuery(
      query.name, url, sqlText, query.description, query.executionMode
    ))
  }
}
