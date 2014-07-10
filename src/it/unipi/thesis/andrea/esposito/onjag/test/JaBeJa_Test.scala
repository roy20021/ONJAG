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

import it.unipi.thesis.andrea.esposito.onjag.protocols._
import it.unipi.thesis.andrea.esposito.onjag.core._
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import it.unipi.thesis.andrea.esposito.onjag.protocols.randompeersampling.RangeRandomPeerSamplingProtocol
import it.unipi.thesis.andrea.esposito.onjag.util.{ApproximateNormalizedMinCut, Random}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


/**
 * Created by Andrea Esposito <and1989@gmail.com> on 21/01/14.
 */
object JaBeJa_Test {

  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis

    if (args.length < 19 || (args(18).toBoolean && args.length < 23)) {
      System.err.println("Usage: app <inputFile> <usePartitioner> <numPartitionsSpark> <maxStep> <host> <spark_home> <checkpointDir> <numPartitionJaBeJa> <seedColoring> <seedProtocol> <alpha_jabeja> <start_SA> <delta_SA> <randomViewSize> <partnerPickOrder> <partner (prob1,prob2,..,probN)> <minCutSamplingRate> <approximate> <useTman> {<tmanViewSize> <tmanStep> <jtmanC> <jtmanR>}")
      System.exit(-1)
    }

    val inputFile = args(0)
    val usePartitioner = args(1).toBoolean
    val numPartitions = args(2).toInt
    val maxStep = args(3).toInt
    val host = args(4)
    val spark_home = args(5)
    val checkpointDir = args(6)
    val numPartitionJabeJa = args(7).toInt
    val seedColoring = args(8).toInt
    val seedProtocol = args(9).toInt
    val alpha_jabeja = args(10).toDouble
    val startSA = args(11).toDouble
    val deltaSA = args(12).toDouble
    val randomViewJabeJaSize = args(13).toInt
    val pickOrder = args(14) match {
      case "TMAN_NEIGH_RAND" => PartnerPickOrder.TMAN_NEIGH_RAND
      case "RAND_TMAN_NEIGH" => PartnerPickOrder.RAND_TMAN_NEIGH
      case "NEIGH_TMAN_RAND" => PartnerPickOrder.NEIGH_TMAN_RAND
    }
    val partnerProbabilities = args(15).substring(1, args(15).length - 1).split(",").map(str => str.toDouble)
    val minCutSamplingRate = args(16).toInt
    val approximate = args(17).toBoolean
    val useTmanFlag = args(18).toBoolean
    var tmanViewJaBeJaSize: Int = 0
    var tmanStep: Int = 1
    var jtmanC: Int = 0
    var jtmanR: Int = 0

    if (useTmanFlag) {
      tmanViewJaBeJaSize = args(19).toInt
      tmanStep = args(20).toInt
      jtmanC = args(21).toInt
      jtmanR = args(22).toInt
    }

    val sc = new SparkContext(host, "OnJag", spark_home, Seq("./ONJAG.jar"))
    sc.setCheckpointDir(checkpointDir)

    // Parse the text file into a graph
    val input = sc.textFile(inputFile)

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices. They are %d".format(numVertices))

    val randomForProtocol = new Random(seedProtocol)
    val randomForColoring = new Random(seedColoring)

    val ids = input.map(line => line.split("\t")(0).toInt).collect() // Let's waste memory! IT IS the convenient way for tests...

    //val peerSamplingProtocol = new P2PRandomPeerSamplingProtocol(sc, 100, 0, 50, 4, true, true, PeerModeSelection.RAND, randomForProtocol.nextLong(), 0, 1)
    val peerSamplingProtocol = new RangeRandomPeerSamplingProtocol(ids.min, ids.max, randomForProtocol.nextLong(), 0, 1)
    val jabejaTMANProtocol = if (useTmanFlag) new JaBeJaTMANProtocol(jtmanC, 0, jtmanR, 0, 1) else new DummyProtocol
    val jabejaProtocol = new JaBeJaProtocol(randomForProtocol.nextLong(), alpha_jabeja, startSA, deltaSA, randomViewJabeJaSize, tmanViewJaBeJaSize, pickOrder, partnerProbabilities, 1, minCutSamplingRate, approximate, 0, tmanStep)
    val onjag = new OnJag(sc, StorageLevel.MEMORY_AND_DISK_SER,
      peerSamplingProtocol,
      jabejaTMANProtocol,
      jabejaProtocol
    )
    if (maxStep > 0)
      onjag.setMaxStep(maxStep)

    println("Parsing input file...")

    def nextColor(): Int = {
      onjag.synchronized {
        (randomForColoring.nextDouble() * numPartitionJabeJa).toInt
      }
    }

    val numEdges = sc.accumulator(0)

    var vertices = input.map(line => {
      val fields = line.split("\t")
      val (id, body) = (fields(0).toInt, fields(1).replace("\\n", "\n"))
      val links = body.split(",").map(strLink => strLink.toInt).map(x => x.asInstanceOf[Any])
      numEdges += links.size
      val data = Array(
        links.asInstanceOf[Array[Any]],
        links.asInstanceOf[Array[Any]],
        links.asInstanceOf[Array[Any]].+:(nextColor)
      )
      (id, onjag.createVertex(id, data))
    })

    if (usePartitioner)
      vertices = vertices.partitionBy(new HashPartitioner(sc.defaultParallelism)).cache
    else
      vertices = vertices.cache
    println("Done parsing input file.")

    vertices.foreach(x => {}) // Force evaluation to compute the numEdges accumulator
    println("Done counting Edges. They are %d (%d undirected)".format(numEdges.value, numEdges.value / 2))

    //Print Initial Graph
    printInitialGraph(vertices, useTmanFlag)

    // Initialization
    val messages = onjag.createInitMessages(vertices, new Array[Any](3))

    ApproximateNormalizedMinCut.maxSupersteps = 500
    val initialNormalizedMinCut = ApproximateNormalizedMinCut.measureFromJaBeJa(sc, vertices, numPartitionJabeJa, 2)
    println("Initial Normalized MinCut = %s".format(initialNormalizedMinCut))

    val checkpointConditions = Seq(Some(new SuperstepIntervalCondition(2)), Some(new SuperstepIntervalCondition(2)), Some(new SuperstepIntervalCondition(2)))

    println("OnJag Execution Started")
    val result = onjag.run(sc, vertices, messages, checkpointConditions, numPartitions, PersistenceMode.DELETE_IMMEDIATELY_SYNCHED)
    println("OnJag Execution Ended")

    // Print the result
    printResultGraph(result, useTmanFlag)

    println("JaBeJa Best Configuration")
    println(jabejaProtocol.bestMinCutList.value.map(t => "(" + t._1 + "," + t._2 + ")\n").mkString)

    println("Final result JaBeJa Monitor")
    println(jabejaProtocol.toString())

    println("Stopping condition used: " + jabejaProtocol.stableMinCut)

    println("Best MinCut Configuration")
    println("Superstep: " + (jabejaProtocol.bestMinCutSuperstep))
    println("Value: " + jabejaProtocol.bestMinCutValue)

    val timeTaken = System.currentTimeMillis - startTime
    println(timeTaken)

    ApproximateNormalizedMinCut.maxSupersteps = -1
    val normalizedMinCut = ApproximateNormalizedMinCut.measureFromJaBeJa(sc, result, numPartitionJabeJa, 2)
    println("Normalized MinCut = %s".format(normalizedMinCut))
  }

  def printInitialGraph[K](vertices: RDD[(K, Vertex[K])], useTmanFlag: Boolean) {
    var initGraph = (vertices.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(0).toString)
    }.collect.mkString)
    //println("Init Graph PeerSampling")
    //println(initGraph)

    if (useTmanFlag) {
      initGraph = (vertices.map {
        case (id, vertex) =>
          "%s\n".format(vertex.protocolsContext(1).toString)
      }.collect.mkString)
      //println("Init Graph JaBeJa TMAN")
      //println(initGraph)
    }

    initGraph = (vertices.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(2).toString)
    }.collect.mkString)
    println("Init Graph JaBeJa")
    println(initGraph)
  }

  def printResultGraph[K](result: RDD[(K, Vertex[K])], useTmanFlag: Boolean) {
    var top = (result.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(0).toString)
    }.collect.mkString)
    //println("Final result PeerSampling")
    //println(top)

    if (useTmanFlag) {
      top = (result.map {
        case (id, vertex) =>
          "%s\n".format(vertex.protocolsContext(1).toString)
      }.collect.mkString)
      println("Final result JaBeJa TMAN")
      println(top)
    }

    top = (result.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(2).toString)
    }.collect.mkString)
    println("Final result JaBeJa")
    println(top)
  }
}
