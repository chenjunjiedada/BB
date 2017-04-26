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

package io.bigdatabenchmark.v1.queries

import java.io.{BufferedOutputStream, OutputStreamWriter}

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Performs Kmeans clustering on data. (used by multiple queries)
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.1 28.10.2015 for spark >= 1.5
 */
object KMeansClustering {

  val CSV_DELIMITER_OUTPUT = "," //same as used in hive engine result table to keep result format uniform

  type OptionMap = Map[Symbol, String]

  var options: OptionMap=Map('iter -> "20",
    'externalInitClusters -> "",
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

    //parse cmd line options and pass defaults for parameters
    options = parseOption(options, args.toList)
    println(s"Run kmeans clustering with options: $options")

    val sc = new SparkContext(new SparkConf().setAppName(options('qnum) + "_KMeansClustering"))

    //pre-cleanup of output file/folder
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(options('output)), true) } catch { case _ : Throwable => { } }



    println(s"read vectors from ${options('input)}")
    //load data and convert to RDD[LabeldPoint]
    val rawInput = if (options('fromHiveMetastore).toBoolean) {
      loadfromHiveMetastore(sc)
    } else {
      loadFromCSV(sc)
    }
    rawInput.cache() //used later when .zip'ing with prediction

    if (options('verbose).toBoolean) {
      println("first 10 line from input:")
      println(rawInput.take(10).deep.mkString("\n") + "\n...")
    }


    //Convert to Vector
    val vectors = rawInput.values.map(Vectors.dense(_))
    vectors.persist() //require to build model and do the prediction

    //configure KMeans
    val means = new KMeans().setK(options('numclust).toInt).setMaxIterations(options('iter).toInt)
    val centersFromFile = !options('externalInitClusters).isEmpty
    if(false && centersFromFile) {

     // intial external clusters only available starting at spark 1.4

      val initialCluster_txt=sc.textFile(options('externalInitClusters))
      val initialVectors= initialCluster_txt.map(s => Vectors.dense(s.split(" ").drop(1).map(_.toDouble))).collect()
      val initialClusters =  new KMeansModel(initialVectors)
      means.setInitialModel(initialClusters)

    } else {
      means.setInitializationMode(KMeans.RANDOM).setSeed(675234312453645L)
    }

    //run clustering
    val clusterModel= means.run(vectors)

    // Evaluate clusterModel by computing Within Set Sum of Squared Errors
    val wwge = clusterModel.computeCost(vectors)

    //classify raw data with craated clustering model
    val prediction= clusterModel.predict(vectors)

    //create & save clustering result result: "<label>,<cluster_id>"
    if(options('saveClassificationResult).toBoolean) {
      val result = prediction.zip(rawInput).map(z => Array(z._2._1, z._1).mkString(CSV_DELIMITER_OUTPUT))
      println("save classification result to " + options('output) + " format: <serialKey>,<cluster>")
      result.saveAsTextFile(options('output))
    }

    //print and write clustering  metadata
    var centersArray: List[Object] = Nil
    centersArray = clusterModel.clusterCenters.toList ::: List("WWGE:" + wwge, "Number of Clusters: " + clusterModel.k, "") ::: centersArray
    val metaInformation= (centersArray.reverse.mkString("Clusters:\n", "\n","\n"))
    println(metaInformation)

    if(options('saveMetaInfo).toBoolean) {
      val metaInfoOutputPath = new Path(options('output), "metainfo.txt")
      println("save meta info to: " + metaInfoOutputPath)
      val out = new OutputStreamWriter (new BufferedOutputStream( hdfs.create(metaInfoOutputPath)))
      out.write(metaInformation)
      out.close()
    }

    sc.stop()
  }

  def loadFromCSV(sc: SparkContext): RDD[(Long, Array[Double])] = {
    println(s"""load from csv file  delimiter("${options('csvInputDelimiter)}"): ${options('input)}""")

    val delimiter = options('csvInputDelimiter)

    val rawInput = sc.textFile(options('input)).map(s => {
      val splits = s.split(delimiter)
      (splits(0).toLong, splits.drop(1).map(_.toDouble))
    })
    rawInput
  }

  /**
   * Expected input:  "label|<feature:double>|<feature:double>|..."
   * @param sc
   * @return
   */
  def loadfromHiveMetastore( sc: SparkContext):  RDD[(Long, Array[Double])] = {
    println(s"""loading data from metastore table: "${options('input)}" ...""")
    val sqlCtx = new HiveContext(sc)
    val inputTable = sqlCtx.table(options('input))
    inputTable.map(s => dataFrameRowToRDD(s))
  }
  /**
   * convertes row from dataFrame to Vectors
   * @param line
   * @return
   */
  def dataFrameRowToRDD( line: Row): (Long,Array[Double]) = {
    val doubleValues = (for (i <- 1 to line.size -1) yield {
      line.getDouble(i)
    }).toArray
    
    (line.getLong(0),doubleValues)
  }
  val usage =
    """
      |Options:
      |[-i  | --input <input dir> OR <database>.<table>]
      |[-o  | --output output folder]
      |[-d  | --csvInputDelimiter <delimiter> (only used if load from csv)]
      |[-ic | --initialClusters initialClustersFile (initial clusters prohibit --iterations > 1)]
      |[-c  | --num-clusters clusters]
      |[-it | --iterations iterations]
      |[-q  | --query-num number for job identification ]
      |[--fromHiveMetastore true=load from hive table | false=load from csv file]
      |[--saveClassificationResult store classification result into HDFS
      |[--saveMetaInfo store metainfo about classification (cluster centers and clustering quality) into HDFS
      |[-v  | --verbose]
      |Defaults:
      |  iterations: 20
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
      case "-o" :: value :: tail => parseOption(map ++ Map('output -> value), tail)
      case "--output" :: value :: tail => parseOption(map ++ Map('output -> value), tail)
      case "-d" :: value :: tail => parseOption(map ++ Map('csvInputDelimiter-> value), tail)
      case "--csvInputDelimiter" :: value :: tail => parseOption(map ++ Map('csvInputDelimiter-> value), tail)
      case "-c" :: value :: tail => parseOption(map ++ Map('numclust -> value), tail)
      case "--num-clusters" :: value :: tail => parseOption(map ++ Map('numclust -> value), tail)
      case "-it" :: value :: tail => parseOption(map ++ Map('iter -> value), tail)
      case "--iterations" :: value :: tail => parseOption(map ++ Map('iter -> value), tail)
      case "-q" :: value :: tail => parseOption(map ++ Map('qnum -> value), tail)
      case "--query-num" :: value :: tail => parseOption(map ++ Map('qnum -> value), tail)
      case "--initialClustersFile"  :: value :: tail => parseOption(map ++ Map('externalInitClusters -> value), tail)
      case "-ic"  :: value :: tail => parseOption(map ++ Map('externalInitClusters -> value), tail)
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
}
