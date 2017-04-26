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

package io.bigdatabenchmark.v1.queries.q28

import java.io.{BufferedOutputStream, OutputStreamWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Success, Try}

/**
 * Performs NaiveBayes classification on data.
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.1 28.10.2015 for spark >= 1.5
 */
object NaiveBayesClassifier {

  val CSV_DELIMITER_OUTPUT = "," //same as used in hive engine result table to keep result format uniform


  type OptionMap = Map[Symbol, String]

  var options: OptionMap=Map('splitRatio -> "",
                            'lambda -> "1.0",
                            'fromHiveMetastore -> "true",
                            'csvInputDelimiter -> ",",
                            'saveClassificationResult -> "true",
                            'saveMetaInfo -> "true",
                            'verbose -> "false"
  )

  def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    if(args.isEmpty){
      println(usage)
      return
    }
    options = parseOption(options, args.toList)
    println(s"Run NaiveBayes with options: $options")

    //pre-cleanup of output file/folder
    val hadoopConf = new Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new Path(options('output)), true) } catch { case _ : Throwable => { } }

    val sc = new SparkContext(new SparkConf().setAppName("q28_nb_classifier"))
    val preSplit=options('splitRatio).isEmpty

    val (trainingLabledPoints, testingLabeldPoints, inputTesting) =  prepareInputData(sc, preSplit)

    println("Training NaiveBayes model")
    val model = NaiveBayes.train(trainingLabledPoints, options('lambda).toDouble)

    println("Testing NaiveBayes model")
    val prediction = testingLabeldPoints.map(p => (model.predict(p.features), p.label)).cache()
    prediction.cache() //required for output and metrics

    val multMetrics = new MulticlassMetrics(prediction)

    val prec = multMetrics.precision
    val confMat = multMetrics.confusionMatrix


    //predict based on created model and store prediction to HDFS
    if(options('saveClassificationResult).toBoolean) {
      println(s"save to ${options('output)}")
      //output review_sk, original_rating, classification_result_string
      prediction.zip(inputTesting).map(p => List(p._2._1._1, p._2._1._2, labelToString(p._1._1)).mkString(CSV_DELIMITER_OUTPUT)).saveAsTextFile(options('output))
    }

    //calculate Metadata about created model
    val metaInformation=
      s"""Precision: $prec
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


  def prepareInputData(sc: SparkContext, preSplit: Boolean): (RDD[LabeledPoint], RDD[LabeledPoint], RDD[((Long,Int), Seq[String])] )= {
    //testdata already in two separate files trainging/testing? if not => split
    if (preSplit) {
      println(s"Input is pre-split, read 'training' from: ${options('inputTrain)} and 'testing' from:  ${options('inputTest)}")

      println("Reading training data")
      val inputTraining = load(options('inputTrain), sc)
      inputTraining.cache()
      val trainingLabledPoints = inputToLabeldPoints(inputTraining)
      inputTraining.unpersist(false)

      println("Reading testing data")
      val inputTesting = load(options('inputTest), sc)
      inputTesting.persist()
      val testingLabeldPoints = inputToLabeldPoints(inputTesting);
      //do not unpersist inputTesting, required later
      (trainingLabledPoints, testingLabeldPoints, inputTesting)
    } else {
      println(s"Input not pre-split, read from: ${options('inputTrain)} and random split into 'training' and 'testing' with ratio: ${options('splitRatio)}")
      val input = load(options('inputTrain), sc)
      val ratio = options('splitRatio).toDouble

      val splits = input.randomSplit(Array(ratio, 1.0 - ratio), seed = 7623425465362456L)
      val inputTraining = splits(0) //format: review_sk,rating[1,5],WordTokensArray
      val inputTesting = splits(1) //format: review_sk,rating[1,5],WordTokensArray

      inputTraining.cache()
      val trainingLabledPoints = inputToLabeldPoints(inputTraining) //format label{NEG=-1, NEUT=0,POS=1}, vector
      inputTraining.unpersist(false)
      inputTesting.persist()
      val testingLabeldPoints = inputToLabeldPoints(inputTesting);

      if(options('verbose).toBoolean){
        println(s"""first 50 of ${inputTraining.count()} lines "inputTraining":""")
        inputTraining.take(50).foreach(println)
        println("...")

        println(s"""first 50 of ${trainingLabledPoints.count()} lines "trainingLabledPoints":""")
        trainingLabledPoints.take(50).foreach(println)
        println("...")

        println(s"""first 50 of ${inputTesting.count()} lines "inputTesting":""")
        inputTesting.take(50).foreach(println)
        println("...")

        println(s"""first 50 of ${testingLabeldPoints.count()} lines "testingLabeldPoints":""")
        testingLabeldPoints.take(50).foreach(println)
        println("...")
      }



      (trainingLabledPoints, testingLabeldPoints, inputTesting)
    }
  }

  def load(inputFileOrTable:String, sc: SparkContext):  RDD[((Long,Int), Seq[String])] = {
    if (options('fromHiveMetastore).toBoolean) {
      loadfromHiveMetastore(inputFileOrTable, sc)
    } else {
       loadFromTextFile(inputFileOrTable, sc)
   }
  }
  def loadfromHiveMetastore(tableName: String, sc: SparkContext): RDD[((Long,Int), Seq[String])] = {

    /*-- input from hive table
       review_sk         BIGINT,
       pr_review_rating  BIGINT,
       pr_review_content STRING
     */

    println(s"""load data from metastore table "$tableName" ...""")
    val sqlCtx = new HiveContext(sc)
    val inputTable = sqlCtx.table(tableName)
    if (options('verbose).toBoolean) {
      println(s"""first 10 lines of "$tableName" :""")
      inputTable.show(10)
    }
    //split review text into terms using delimiter " " (simplistic)
   inputTable.map(row => ((row.getAs[Long](0),row.getAs[Int](1)),row.getAs[String](2).split(" ").toSeq))
  }

  def loadFromTextFile(inputFile: String, sc: SparkContext):RDD[((Long,Int), Seq[String])] = {
    //parse input and remove bad lines
    println(s"""load from csv file  delimiter("${options('csvInputDelimiter)}"): ${inputFile}""")
    val rawRDD=sc.textFile(inputFile)
    if(options('verbose).toBoolean){
      println(s"""first 10 of ${rawRDD.count()} lines raw data from: "$inputFile" :""")
      rawRDD.take(10).foreach(println)
      println("...")
    }
    val csvInputDelimiter = options('csvInputDelimiter)

    val cleandInput = rawRDD.map(parseInputCSVLine(_,csvInputDelimiter)).collect { case Success(value) => value }

    if(options('verbose).toBoolean){
      println(s"""first 10 of ${cleandInput.count()} lines of parsed and cleand data from: "$inputFile" :""")
      cleandInput.take(10).foreach(println)
      println("...")
    }
    cleandInput
  }
  /**
   * IN: <pr_review_sk>\t<pr_rating>\t<pr_review_content>
   * OUT: (<pr_review_sk:long>, <pr_rating:int>, <terms:Seq[String]>)
   * @param inputLine
   * @return
   */
  def parseInputCSVLine(inputLine:String,delimiter:String): Try[((Long,Int),Seq[String])] ={

    val parsed = util.Try{
      //split review text into terms using delimiter " " (simplistic)

      //val splits = inputLine.split(CSV_DELIMITER_INPUT)
      //((splits(0).toLong, labelToRating(splits(1))), splits(2).split(" ").toSeq)

      //manual split instead of inputLine.split(CSV_DELIMITER_INPUT), in case review texts contains the delimiter
      var nextStart = 0
      var nextDelim = inputLine.indexOf(delimiter)

      val reviewSk = inputLine.substring(nextStart, nextDelim).toLong
      nextStart = nextDelim +1
      nextDelim = inputLine.indexOf(delimiter,nextStart)
      val rating=labelToRating(inputLine.substring(nextStart,nextDelim))
      nextStart=nextDelim+1
      val review = inputLine.substring(nextStart,inputLine.length)
      val tokens = review.split(" ").toSeq

      ((reviewSk,rating),tokens)
    }
    parsed.failed.foreach(e => println("Skipping bad line: " + inputLine + "\n"+e))
    parsed
  }

  def inputToLabeldPoints(ratingAndContent: RDD[((Long,Int), Seq[String])]): RDD[LabeledPoint] = {

    val termValues=ratingAndContent.values;

    //calc TFIDF from input terms
    val tfidf: RDD[linalg.Vector] = tfidfFromTerms(termValues)

    println(s"Create Labled Points(rating, FeatureVector))")
    val points = tfidf.zip(ratingAndContent).map(t => {
      val rating= t._2._1._2 //from right zip "ratingAndContent" (._2) get key(review_sk, rating) (._1) and from key get rating (._2)
      new LabeledPoint(ratingToDoubleLabel(rating), t._1)
    })
    points
  }

  def tfidfFromTerms(termValues: RDD[Seq[String]]): RDD[linalg.Vector] = {
    println(s"calculcate TF")
    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(termValues)
    tf.cache()

    println(s"calculcate TFIDF 1/2 => IDF.fit(TF)")
    val idf = new IDF().fit(tf)
    println(s"calculcate TFIDF 2/2 => transform IDF to TFIDF")
    val tfidf = idf.transform(tf)
    tf.unpersist(false)
    tfidf
  }

  val usage =
    """
      |Options:
      |[-i | --inputTraining <input dir> OR <database>.<table>]
      |[-t | --inputTesting --input <input dir> OR <database>.<table>]
      |[-o | --output outputfolder]
      |[-d | --csvInputDelimiter <delimiter> (only used if load from csv)]
      |[-r | --training-ratio ratio (if ratio e.g. 0.3 is provided, -t (testing) is ignored and -i (training) is split into training (e.g 0.3) and classification (e.g. 0.7))) ]
      |[-l | --lambda ]
      |[--fromHiveMetastore true=load from hive table | false=load from csv file]
      |[--saveClassificationResult store classification result into HDFS]
      |[--saveMetaInfo store metainfo about classification (cluster centers and clustering quality) into HDFS]
      |[-v  | --verbose]
      |Defaults:
      |  lambda: 1
      |  fromHiveMetastore: true
      |  saveClassificationResult: true
      |  saveMetaInfo: true
      |  verbose: false
    """.stripMargin

  def parseOption(map: OptionMap, args: List[String]) : OptionMap = {
    args match {
      case Nil => map
      case "-i" :: value :: tail => parseOption(map ++ Map('inputTrain -> value), tail)
      case "--inputTraining" :: value :: tail => parseOption(map ++ Map('inputTrain -> value), tail)
      case "-t" :: value :: tail => parseOption(map ++ Map('inputTest -> value), tail)
      case "--inputTesting" :: value :: tail => parseOption(map ++ Map('inputTest -> value), tail)
      case "-o" :: value :: tail => parseOption(map ++ Map('output -> value), tail)
      case "--output" :: value :: tail => parseOption(map ++ Map('output -> value), tail)
      case "-d" :: value :: tail => parseOption(map ++ Map('csvInputDelimiter-> value), tail)
      case "--csvInputDelimiter" :: value :: tail => parseOption(map ++ Map('csvInputDelimiter-> value), tail)
      case "-r" :: value :: tail => parseOption(map ++ Map('splitRatio -> value), tail)
      case "--training-ratio" :: value :: tail => parseOption(map ++ Map('splitRatio -> value), tail)
      case "-l" :: value :: tail => parseOption(map ++ Map('lambda -> value), tail)
      case "--lambda" :: value :: tail => parseOption(map ++ Map('lambda -> value), tail)
      case "--fromHiveMetastore" :: value :: tail => parseOption(map ++ Map('fromHiveMetastore -> value), tail)
      case "--saveClassificationResult" :: value :: tail => parseOption(map ++ Map('saveClassificationResult -> value), tail)
      case "--saveMetaInfo" :: value :: tail => parseOption(map ++ Map('saveMetaInfo -> value), tail)
      case "-v" :: value :: tail => parseOption(map ++ Map('verbose -> value), tail)
      case "--verbose" :: value :: tail => parseOption(map ++ Map('verbose -> value), tail)
      case option :: optionTwo :: tail =>
        println("Bad Option " + option)
        println(usage)
        map
    }
  }

  def ratingToDoubleLabel(label: Int): Double = {
    label match {
      case 1 => -1.0
      case 2 => -1.0
      case 3 => 0.0
      case 4 => 1.0
      case 5 => 1.0
      case _ => 0.0
    }
  }

  def labelToRating(label: String): Int = {
    label match {
      case "NEG"  => 1
      case "NEUT" => 3
      case "POS"  => 5
      case _ => 3
    }
  }

  def ratingToString(label: Int): String = {
    label match {
      case 1 => "NEG"
      case 2 => "NEG"
      case 3 => "NEUT"
      case 4 => "POS"
      case 5 => "POS"
      case _ =>"NEUT"
    }
  }


  def labelToString(value: Double): String = {
    value match {
      case -1.0  => "NEG"
      case 1.0  => "POS"
      case _ =>  "NEUT"
    }
  }
}
