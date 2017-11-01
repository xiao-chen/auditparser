/**
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

package com.cloudera.experiment

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions._
import scala.collection.mutable.Stack


/**
  * Class to analyse audit logs in Navigator-compatible json format.
  * <p>
  * Run with:
  *   spark2-submit --class com.cloudera.experiment.AuditLogJsonAnalyser --deploy-mode client --master yarn ~/logparser-1.0.jar <filename>
  */
object AuditLogJsonAnalyser {

  // Navigator fields
  val ALLOWED = "allowed"
  val EVENT_TIME = "eventTime"
  val USERNAME = "username"
  val SRC = "src"
  val DEST = "dest"
  val IP_ADDRESS = "ipAddress"
  val OPERATION = "operation"
  val PERMISSIONS = "permissions"
  val IMPERSONATOR = "impersonator"
  val DELEGATION_TOKEN_ID = "delegationTokenId"
  val SERVICE_NAME = "serviceName"

  var outFile : FSDataOutputStream = null
  var spark : SparkSession = null

  def main(args: Array[String]) {

    if (args.length < 1) {
      sys.error("Usage: " + this.getClass.getName + " <json file or dir>")
    }
    spark = SparkSession
      .builder
      .appName("SparkKMeans")
      .getOrCreate()

    var fs = FileSystem.get(new Configuration())
    var path = new Path("hdfs:///result_" + System.currentTimeMillis())
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    outFile = fs.create(path)

    val inputPathName = args(0)
    val inputPath = new Path(inputPathName)
    var status = fs.getFileStatus(inputPath)
    var dirs : Stack[FileStatus] = Stack()

    def processDir(path: Path) = {
      var iter = fs.listStatusIterator(path)
      while (iter.hasNext()) {
        status = iter.next();
        println("____ processing " + status)
        if (status.isFile) {
          analyseFile(status.getPath)
        } else if (status.isDirectory) {
          dirs.push(status)
        }
      }
    }

    if (status.isDirectory) {
      processDir(inputPath)
      while (dirs.nonEmpty) {
        processDir(dirs.pop().getPath)
      }
    } else {
      analyseFile(inputPath)
    }

    if (outFile != null) {
      outFile.close()
    }
    spark.stop()
  }

  def analyseFile(file: Path): Unit = {
    writeToOutput("\n\n**** Analyzing file " + file.toString + " ****\n")
    assert(spark != null)
    try {
      val df = spark.read.json(file.toString)
      analyzeColumn(USERNAME, df)
      analyzeColumn(IP_ADDRESS, df)
      analyzeColumn(OPERATION, df)
    } catch {
      case ae: AnalysisException => println("**** filed to analyze file " + file.toString)
      case e : Exception => throw e
    }
  }

  def analyzeColumn(colName : String, df: DataFrame) : Unit = {
    writeToOutput("\n**** Counting by " + colName + "****\n")
    var result = df.select(USERNAME, IP_ADDRESS, OPERATION, SRC, DEST, EVENT_TIME)
      .groupBy(colName).count().orderBy(desc("Count"))

    result.show(false)
    writeToOutput(result.rdd.map(_.mkString("|")).collect().mkString("\n"))
    writeToOutput("\n\n")
  }

  def writeToOutput(content : String): Unit = {
    assert(outFile != null)
    outFile.writeBytes(content)
  }

  def getOutputLocation() : String = {
    "hdfs:///result_" + System.currentTimeMillis()
  }
}
