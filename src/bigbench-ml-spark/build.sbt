//"INTEL CONFIDENTIAL"
//Copyright 2016 Intel Corporation All Rights Reserved.
//
//The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
//
//No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

name := "bigBench-MLLib"

version := "1.0"

scalaVersion := "2.10.2"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.2"

// protocol buffer support
//seq(sbtprotobuf.ProtobufPlugin.protobufSettings: _*)


// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.0",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.0",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.0",
  //"org.apache.spark" % "spark-unsafe_2.10" % "1.5.0",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0"
  //"org.apache.commons" % "commons-lang3" % "3.0",
  //"org.eclipse.jetty"  % "jetty-client" % "8.1.14.v20131031",
  //"com.typesafe.play" % "play-json_2.10" % "2.2.1",
  //"org.elasticsearch" % "elasticsearch-hadoop-mr" % "2.0.0.RC1",
  //"net.sf.opencsv" % "opencsv" % "2.0",
  //"com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.17",
  //"org.scalatest" %% "scalatest" % "2.2.1" % "test",
  //"com.holdenkarau" %% "spark-testing-base" % "0.0.1" % "test"
)

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)
