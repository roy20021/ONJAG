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

package it.unipi.thesis.andrea.esposito.onjag.test.fake

import it.unipi.thesis.andrea.esposito.onjag.protocols._
import it.unipi.thesis.andrea.esposito.onjag.core._
import org.apache.spark.{SparkContext, AccumulatorParam, HashPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 06/02/14.
 */
object KCoreMain {

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
    val sc = new SparkContext(host, "OnJag", spark_home, Seq("./ONJAG.jar"))
    sc.setCheckpointDir(checkpointDir)

    // Parse the text file into a graph
    val input = sc.textFile(inputFile)

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices. They are %d".format(numVertices))

    val kcoreProtocol = new KCorenessProtocol(0, 1)

    val onjag = new OnJag(sc, OnJag.DEFAULT_STORAGE_LEVEL, kcoreProtocol)
    if (maxStep > 0)
      onjag.setMaxStep(maxStep)

    val numEdges = sc.accumulator(0)(new AccumulatorParam[Int] {
      def zero(initialValue: Int): Int = initialValue

      def addInPlace(r1: Int, r2: Int): Int = r1 + r2
    })

    println("Parsing input file...")
    var vertices = input.map(line => {
      val fields = line.split("\t")
      val (id, body) = (fields(0).toInt, fields(1).replace("\\n", "\n"))
      val links = body.split(",").map(strLink => strLink.toInt).map(x => x.asInstanceOf[Any])
      numEdges += links.size
      val data = Array(links.asInstanceOf[Array[Any]])
      (id, onjag.createVertex(id, data))
    })

    if (usePartitioner)
      vertices = vertices.partitionBy(new HashPartitioner(sc.defaultParallelism)).cache
    else
      vertices = vertices.cache
    println("Done parsing input file.")

    println("Done counting Edges. They are %d (%d undirected)".format(numEdges.value, numEdges.value / 2))

    //Print Initial Graph
    printInitialGraph(vertices)

    // Initialization
    val messages = onjag.createInitMessages(vertices, new Array[Any](1))

    println("OnJag Execution Started")
    val result = onjag.run(sc, vertices, messages, numPartitions)
    println("OnJag Execution Ended")

    // Print the result
    printResultGraph(result)

    println("Max Msg Sent per Node: %s".format(kcoreProtocol.maxMsgsSent))
    println("Average Msg Sent per Node: %s".format(kcoreProtocol.avgMsgsSent))

    val timeTaken = System.currentTimeMillis - startTime
    println(timeTaken)
  }

  def printInitialGraph[K](vertices: RDD[(K, Vertex[K])]) {

    val initGraph = (vertices.map {
      case (id, vertex) => "%s\t%d\n".format(id, (vertex.protocolsContext(0).asInstanceOf[KCorenessVertexContext[Int]]).kcoreness)
    }.collect.mkString)
    println("Init Graph KCore")
    println(initGraph)
  }

  def printResultGraph[K](result: RDD[(K, Vertex[K])]) {
    val top = (result.map {
      case (id, vertex) =>
        "%s\t%d\n".format(id, (vertex.protocolsContext(0).asInstanceOf[KCorenessVertexContext[Int]]).kcoreness)
    }.collect.mkString)
    println("Final result KCore")
    println(top)

    val kcores = result.map {
      case (id, vertex) => vertex.protocolsContext(0).asInstanceOf[KCorenessVertexContext[Int]].kcoreness
    }

    val maxKCore = kcores.fold(0)((prec, kcore) => if (prec < kcore) kcore else prec)
    val averageKCore: Double = kcores.fold(0)((prec, kcore) => prec + kcore).toDouble / kcores.count()

    println("Max KCore: %s".format(maxKCore.toString))
    println("Average KCore: %s".format(averageKCore.toString))
  }
}
