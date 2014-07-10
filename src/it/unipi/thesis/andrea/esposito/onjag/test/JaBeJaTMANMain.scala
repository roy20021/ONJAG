/**
 * Copyright 2014 Andrea Esposito <and1989@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.unipi.thesis.andrea.esposito.onjag.test

import org.apache.spark.{HashPartitioner, SparkContext}
import it.unipi.thesis.andrea.esposito.onjag.protocols._
import it.unipi.thesis.andrea.esposito.onjag.core.{OnJag, Vertex}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import it.unipi.thesis.andrea.esposito.onjag.protocols.randompeersampling.{PeerModeSelection, P2PRandomPeerSamplingProtocol}
import it.unipi.thesis.andrea.esposito.onjag.util.Random

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 07/02/14.
 */
object JaBeJaTMANMain {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis

    if (args.length < 6) {
      System.err.println("Usage: app <inputFile> <usePartitioner> <numPartitions> <maxStep> <host> <spark_home> <checkpointDir>")
      System.exit(-1)
    }

    val inputFile = args(0)
    val usePartitioner = args(1).toBoolean
    val numPartitions = args(2).toInt
    val maxStep = args(3).toInt
    val host = args(4)
    val spark_home = args(5)
    val checkpointDir = args(6)
    val sc = new SparkContext(host, "OnJag", spark_home, List("./ONJAG.jar"))
    sc.setCheckpointDir(checkpointDir)

    // Parse the text file into a graph
    val input = sc.textFile(inputFile)

    val random = new Random(2014)

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices. They are %d".format(numVertices))

    val peerProtocol = new P2PRandomPeerSamplingProtocol(8, 0, 4, 2, true, true, PeerModeSelection.TAIL, random.nextLong())
    val tmanProtocol = new JaBeJaTMANProtocol(4, 0, 1)
    val idleProtocol = new IdleProtocol()

    val onjag = new OnJag(sc, OnJag.DEFAULT_STORAGE_LEVEL,
      peerProtocol,
      tmanProtocol,
      idleProtocol)
    if (maxStep > 0)
      onjag.setMaxStep(maxStep)

    println("Parsing input file...")
    var vertices = input.map(line => {
      val fields = line.split("\t")
      val (id, body) = (fields(0).toInt, fields(1).replace("\\n", "\n"))
      val links = body.split(",").map(strLink => strLink.toInt).map(x => x.asInstanceOf[Any])
      val data = Array(
        links.asInstanceOf[Array[Any]],
        links.+:(numVertices).+:(id).asInstanceOf[Array[Any]],
        links.asInstanceOf[Array[Any]]
      )
      (id, onjag.createVertex(id, data))
    })

    if (usePartitioner)
      vertices = vertices.partitionBy(new HashPartitioner(sc.defaultParallelism)).cache
    else
      vertices = vertices.cache
    println("Done parsing input file.")

    //Print Initial Graph
    printInitialGraph(vertices)

    // Initialization
    val messages = onjag.createInitMessages(vertices, new Array[Any](3))

    println("OnJag Execution Started")
    val result = onjag.run(sc, vertices, messages, numPartitions)
    println("OnJag Execution Ended")

    // Print the result
    printResultGraph(result)

    val timeTaken = System.currentTimeMillis - startTime
    println(timeTaken)
  }

  def printInitialGraph[K](vertices: RDD[(K, Vertex[K])]) {
    var initGraph = (vertices.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(0).toString)
    }.collect.mkString)
    println("Init Graph PeerSampling")
    println(initGraph)

    initGraph = (vertices.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(1).toString)
    }.collect.mkString)
    println("Init Graph JaBeJa TMAN")
    println(initGraph)
  }

  def printResultGraph[K](result: RDD[(K, Vertex[K])]) {
    var top = (result.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(0).toString)
    }.collect.mkString)
    println("Final result PeerSampling")
    println(top)

    top = (result.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(1).toString)
    }.collect.mkString)
    println("Final result JaBeJa TMAN")
    println(top)
  }
}
