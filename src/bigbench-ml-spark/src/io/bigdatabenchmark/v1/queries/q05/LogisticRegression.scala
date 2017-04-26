/*
 * "INTEL CONFIDENTIAL" Copyright 2016 Intel Corporation All Rights
 * Reserved.
 *
 * The source code contained or described herein and all documents related
 * to the source code ("Material") are owned by Intel Corporation or its
 * suppliers or licensors. Title to the Material remains with Intel
 * Corporation or its suppliers and licensors. The Material contains trade
 * secrets and proprietary and confidential information of Intel or its
 * suppliers and licensors. The Material is protected by worldwide copyright
 * and trade secret laws and treaty provisions. No part of the Material may
 * be used, copied, reproduced, modified, published, uploaded, posted,
 * transmitted, distributed, or disclosed in any way without Intel's prior
 * express written permission.
 *
 * No license under any patent, copyright, trade secret or other
 * intellectual property right is granted to or conferred upon you by
 * disclosure or delivery of the Materials, either expressly, by
 * implication, inducement, estoppel or otherwise. Any license under such
 * intellectual property rights must be express and approved by Intel in
 * writing.
 */

package io.bigdatabenchmark.v1.queries.q05

import java.io.{BufferedOutputStream, OutputStreamWriter, BufferedWriter}

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Performs Logistic Regression analysis on data.
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.1 28.10.2015 for spark >= 1.5
*/
object LogisticRegression {
  type OptionMap = Map[Symbol, String]

  val CSV_DELIMITER_OUTPUT = "," //same as used in hive engine result table to keep result format uniform


  def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)

    if(args.isEmpty){
      println(usage)
      return
    }

    /*- input table layout:
    clicks_in_category BIGINT,-- label
    college_education  BIGINT,
    male               BIGINT,
    clicks_in_1        BIGINT,
    clicks_in_2        BIGINT,
    clicks_in_7        BIGINT,
    clicks_in_4        BIGINT,
    clicks_in_5        BIGINT,
    clicks_in_6        BIGINT
   */

    //parse command line options and provide a map of default values
    val options = parseOption(Map(
      'stepsize -> "1.0",
      'type -> "LBFGS",
      'iter -> "20",
      'lambda -> "0.0",
      'numClasses -> 2.toString,
      'numCorrections -> 10.toString,
      'convergenceTol -> 1e-5.toString,
      'fromHiveMetastore -> "true",
      'saveClassificationResult -> "true",
      'saveMetaInfo -> "true",
      'verbose -> "false",
      'csvInputDelimiter -> ","
    ), args.toList)
    println(s"Run LogisticRegression with options: $options")

    //pre-cleanup of output file/folder
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(options('output)), true) } catch { case _ : Throwable => { } }


    val conf = new SparkConf().setAppName("q05_logisticRegression")
    val sc = new SparkContext(conf)

    //load data and convert to RDD[LabeldPoint]
    val (average, points) = if (options('fromHiveMetastore).toBoolean) {
      loadfromHiveMetastore(options, sc)
    } else {
      loadFromTextFile(options, sc)
    }


    //statistics of data (debug)
    if (options('verbose).toBoolean) {
      println("Average click count: " + average)
      println("Data Loaded: " + points.count() + " points.")
      println("Interested Count: " + points.filter(l => l.label == 1.0).count())
      println("Uninterest Count: " + points.filter(l => l.label == 0.0).count())
    }

    //train model with data using either SGD or LBFGS LogisticRegression
    println("Training Model")
    val model = getLogistcRegressionModel(options,points)


    //perform prediction based on model
    println("Predict with model")
    val predictionAndLabels = points.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }
    predictionAndLabels.persist() //required for output and statistics on classification

    //store prediction to HDFS
    if(options('saveClassificationResult).toBoolean) {
      println("save prediction to " + options('output))
      predictionAndLabels.saveAsTextFile(options('output))
    }


    //calculate Metadata about created model
    val binMetrics = new BinaryClassificationMetrics(predictionAndLabels)
    val multMetrics = new MulticlassMetrics(predictionAndLabels)

    val prec = multMetrics.precision
    val auc = binMetrics.areaUnderROC()
    val confMat = multMetrics.confusionMatrix

    val metaInformation =
      s"""Precision: $prec
         |AUC: $auc
         |Confusion Matrix:
         |$confMat
         |""".stripMargin

    println(metaInformation)

    //store to HDFS
    if(options('saveMetaInfo).toBoolean) {
      val metaInfoOutputPath = new Path(options('output), "metainfo.txt")
      println("save to " + metaInfoOutputPath)
      val out = new OutputStreamWriter (new BufferedOutputStream( hdfs.create(metaInfoOutputPath)))
      out.write(metaInformation)
      out.close()
    }

    sc.stop()
  }


  def loadFromTextFile(options: OptionMap, sc: SparkContext): (Double, RDD[LabeledPoint]) = {
    println(s"""load from csv file  delimiter("${options('csvInputDelimiter)}"): ${options('input)}""")

    val csvData = sc.textFile(options('input))
    csvData.cache() //don't know yet if caching is beneficial for runtime in this case
    val csvInputDelimiter = options('csvInputDelimiter)
    //clicks are in first column, cols delimited by "csvInputDelimiter"
    val averageClickCount = csvData.map(line => line.substring(0,line.indexOf(csvInputDelimiter)).toDouble ).mean()

    val points: RDD[LabeledPoint] = csvData.map(line => csvLineToLP(line, averageClickCount,csvInputDelimiter)).cache()
    csvData.unpersist(false)
    (averageClickCount, points)
  }

  /**
   * load data from hive metastore table. !!not tested!!
   *
   * @param options uses the "-if" option as input for hive metastore table name in format: "<db>.<table>"
   * @param sc
   * @return
   */
  def loadfromHiveMetastore(options: OptionMap, sc: SparkContext):  (Double, RDD[LabeledPoint]) = {
    println(s"""loading data from metastore table: "${options('input)}" ...""")
    val sqlCtx = new HiveContext(sc)
    val inputTable = sqlCtx.table(options('input))
    if (options('verbose).toBoolean) {
      println(s"first 10 lines of ${options('input)}:")
      inputTable.show(10)
    }
    val averageOfFirstRow = inputTable.select(avg(inputTable.col("clicks_in_category"))).head()
    println(s"average: $averageOfFirstRow")
    val average = averageOfFirstRow.getDouble(0)
    val points: RDD[LabeledPoint] = inputTable.map(line => dataFrameRowToLP(line, average)).cache()
    (average, points)
  }


  def getLogistcRegressionModel(options: OptionMap, points:RDD[LabeledPoint]): LogisticRegressionModel = {
    val gradType = options('type)
    gradType match {
      case "SGD" =>
        val regression = new LogisticRegressionWithSGD()
        regression.optimizer.setNumIterations(options('iter).toInt).setConvergenceTol(options('convergenceTol).toDouble).setRegParam(options('lambda).toDouble).setStepSize(options('stepsize).toDouble)
        regression.run(points)
      case _ =>
        val regression = new LogisticRegressionWithLBFGS().setNumClasses(options('numClasses).toInt)
        regression.optimizer.setNumIterations(options('iter).toInt).setConvergenceTol(options('convergenceTol).toDouble).setRegParam(options('lambda).toDouble).setNumCorrections(options('numCorrections).toInt)
        regression.run(points)
    }
  }

  /**
   * convertes row from dataFrame to LabeldPoint
   * @param line
   * @param mean
   * @return
   */
  def dataFrameRowToLP(line: Row, mean: Double): LabeledPoint = {
    val doubleValues = (for (i <- 0 to line.size -1) yield {
      line.getLong(i).toDouble
    }).toArray

    val label = if (doubleValues(0) > mean) {1.0} else {0.0}
    LabeledPoint(label, Vectors.dense(doubleValues))
  }

  /**
   * Convertes a line of text, delimited by " "
   * @param s
   * @param mean
   * @return
   */
  def csvLineToLP(s: String, mean: Double, delimiter: String): LabeledPoint = {
    val doubleValues = s.split(delimiter).map(_.toDouble)
    val label = if (doubleValues(0) > mean) {1.0} else {0.0}
    LabeledPoint(label, Vectors.dense(doubleValues))
  }


  val usage =
    """
      |Options:
      |[-i  | --input <input dir> OR <database>.<table>]
      |[-o  | --output output folder]
      |[-d  | --csvInputDelimiter <delimiter> (only used if load from csv)]
      |[--type LBFGS|SGD]
      |[-it | --iterations iterations]
      |[-l  | --lambda regularizationParameter]
      |[-n  | --numClasses ]
      |[-t  | --convergenceTol ]
      |[-c  | --numCorrections (LBFGS only) ]
      |[-s  | --step-size size (SGD only)]
      |[--fromHiveMetastore true=load from hive table | false=load from csv file]
      |[--saveClassificationResult store classification result into HDFS
      |[--saveMetaInfo store metainfo about classification (cluster centers and clustering quality) into HDFS
      |[-v  | --verbose]
      |Defaults:
      |  step size: 1.0 (only used with --type sgd)
      |  type: LBFGS
      |  iterations: 20
      |  lambda: 0
      |  numClasses: 2
      |  numCorrections: 10
      |  convergenceTol: 1e-5.
      |  fromHiveMetastore: true
      |  saveClassificationResult: true
      |  saveMetaInfo: true
      |  verbose: false
    """.stripMargin

  def parseOption(map: OptionMap, args: List[String]) : OptionMap = {
    args match {
      case Nil => map
      case "-i" :: value :: tail => parseOption(map ++ Map('input -> value), tail)
      case "--input" :: value :: tail => parseOption(map ++ Map('input -> value), tail)
      case "-o" :: value :: tail => parseOption(map ++ Map('output-> value), tail)
      case "--output" :: value :: tail => parseOption(map ++ Map('output-> value), tail)
      case "-d" :: value :: tail => parseOption(map ++ Map('csvInputDelimiter-> value), tail)
      case "--csvInputDelimiter" :: value :: tail => parseOption(map ++ Map('csvInputDelimiter-> value), tail)
      case "--type" :: value :: tail => parseOption(map ++ Map('type -> value), tail)
      case "-s" :: value :: tail => parseOption(map ++ Map('stepsize -> value), tail)
      case "--step-size" :: value :: tail => parseOption(map ++ Map('stepsize -> value), tail)
      case "-it" :: value :: tail => parseOption(map ++ Map('iter -> value), tail)
      case "--iterations" :: value :: tail => parseOption(map ++ Map('iter -> value), tail)
      case "-l" :: value :: tail => parseOption(map ++ Map('lambda -> value), tail)
      case "--lambda" :: value :: tail => parseOption(map ++ Map('lambda -> value), tail)
      case "-n" :: value :: tail => parseOption(map ++ Map('numClasses -> value), tail)
      case "--numClasses" :: value :: tail => parseOption(map ++ Map('numClasses -> value), tail)
      case "-c" :: value :: tail => parseOption(map ++ Map('numCorrections -> value), tail)
      case "--numCorrections" :: value :: tail => parseOption(map ++ Map('numCorrections -> value), tail)
      case "-t" :: value :: tail => parseOption(map ++ Map('convergenceTol -> value), tail)
      case "--convergenceTol" :: value :: tail => parseOption(map ++ Map('convergenceTol -> value), tail)
      case "--fromHiveMetastore" :: value :: tail => parseOption(map ++ Map('fromHiveMetastore -> value), tail)
      case "--saveClassificationResult" :: value :: tail => parseOption(map ++ Map('saveClassificationResult -> value), tail)
      case "--saveMetaInfo" :: value :: tail => parseOption(map ++ Map('saveMetaInfo -> value), tail)
      case "-v" :: value :: tail => parseOption(map ++ Map('verbose -> value), tail)
      case "--verbose" :: value :: tail => parseOption(map ++ Map('verbose -> value), tail)
      case option :: tail =>
        println("Bad Option " + option)
        println(usage)
        map
    }
  }
}