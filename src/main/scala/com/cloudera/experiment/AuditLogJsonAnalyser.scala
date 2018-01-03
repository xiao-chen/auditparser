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

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions._

import scala.collection.mutable.Stack
import org.slf4j.LoggerFactory

//spark2-submit --class com.cloudera.experiment.AuditLogJsonAnalyser --deploy-mode client --master yarn --num-executors 6 --executor-cores 2 --executor-memory 1G ~/logparser-1.0.jar /audits/0925

/**
  * Class to analyse audit logs in Navigator-compatible json format.
  * <p>
  * Run with:
  *   spark2-submit --class com.cloudera.experiment.AuditLogJsonAnalyser --deploy-mode client --master yarn ~/logparser-1.0.jar <filename>
  */
object AuditLogJsonAnalyser {
  val LOG = LoggerFactory.getLogger("AuditLogJsonAnalyser")

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
      .appName("AuditLogJsonAnalyser")
      .getOrCreate()

    var fs = FileSystem.get(new Configuration())
    var path = new Path("hdfs:///result_" + System.currentTimeMillis())
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    outFile = fs.create(path)

    val inputPathName = args(0)
    val interval = args(1)
    // TODO: validate interval
    val inputPath = new Path(inputPathName)
    var status = fs.getFileStatus(inputPath)
    var dirs : Stack[FileStatus] = Stack()

    def processDir(path: Path) = {
      var iter = fs.listStatusIterator(path)
      while (iter.hasNext()) {
        status = iter.next();
        LOG.info("Processing " + status.getPath)
        if (status.isFile) {
          analyseFile(status.getPath)
        } else if (status.isDirectory) {
          dirs.push(status)
        }
      }
    }

    LOG.info("Analysing audits from {}", inputPathName)
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
    writeToOutput("\n\n**** File " + file.toString + ":\n")
    LOG.info("Analyzing file {}", file)
    assert(spark != null)
    try {
      val dfCache = spark.read.json(file.toString).select(USERNAME, IP_ADDRESS, OPERATION, SRC, DEST, EVENT_TIME)
      LOG.info("Successfully read file")
      dfCache.cache()
      LOG.info("Successfully cached file")
      analyzeColumn(USERNAME, dfCache)
      analyzeColumn(IP_ADDRESS, dfCache)
      analyzeColumn(OPERATION, dfCache)
      aggregateByTime(1000, dfCache)
    } catch {
      case ae: AnalysisException => LOG.error("Failed to analyze file " + file.toString)
      case e : Exception => throw e
    }
  }

  def analyzeColumn(colName : String, df: DataFrame) : Unit = {
    writeToOutput("\n**** Counting by " + colName + ":\n")
    LOG.info("Analyzing column {}", colName)
    var result = df.groupBy(colName).count().orderBy(desc("Count"))
    writeToOutput(result.rdd.map(_.mkString("|")).collect().mkString("\n"))
    writeToOutput("\n\n")
  }

  def aggregateByTime(interval : Int, df: DataFrame) : Unit = {
    writeToOutput("\n**** Aggregation per " + interval / 1000 + " second:\n")
    var startTime : Long = 0
    var boundary : Long = 0
    var count = 0;
    val iter = df.toLocalIterator()
    while (iter.hasNext) {
      val eventTime : Long = iter.next().getAs(EVENT_TIME)
      if (startTime == 0) {
        startTime = eventTime - (eventTime % interval)
        boundary = startTime + interval
        LOG.info(s"____ eventTime=$eventTime, starttime=$startTime, boundary=$boundary")
      }
      count += 1
      if (eventTime > boundary) {
        writeToOutput(timeToStr(boundary) + "|" + count + "\n");
        boundary = eventTime - (eventTime % interval) + interval
        count = 0
        LOG.info(s"____ eventTime=$eventTime, boundary=$boundary, readable={}", timeToStr(boundary))
      }
    }

//    var result = df.groupBy(EVENT_TIME).count().orderBy(desc("Count")).limit(20)
//    writeToOutput("\n**** Aggregation:\n")
//    writeToOutput(result.rdd.map(_.mkString("|")).collect().mkString("\n"))
//    writeToOutput("\n\n")
    LOG.info("Aggregation done on per {} seconds", interval / 1000)
  }

  def writeToOutput(content : String): Unit = {
    assert(outFile != null)
    outFile.writeBytes(content)
  }

  def timeToStr(epochMillis: Long): String =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(epochMillis)

  def getOutputLocation() : String = {
    "hdfs:///result_" + System.currentTimeMillis()
  }
}
