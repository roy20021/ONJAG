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

import org.apache.spark.{SparkContext, HashPartitioner}
import it.unipi.thesis.andrea.esposito.onjag.protocols._
import it.unipi.thesis.andrea.esposito.onjag.core._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import scala.reflect.ClassTag
import it.unipi.thesis.andrea.esposito.onjag.protocols.randompeersampling.{PeerModeSelection, P2PRandomPeerSamplingVertexContext, P2PRandomPeerSamplingProtocol}
import java.io.{DataOutputStream, FileOutputStream}
import it.unipi.thesis.andrea.esposito.onjag.util.Random
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 06/02/14.
 */
object RandomPeerSamplingMain extends Logging {

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

    // Save the sequence on a file
    val file = new DataOutputStream(new FileOutputStream("rndSeq.bin"))
    def writeRnd(num: Int): Unit = {
      this.synchronized {
        file.writeInt(num)
        file.flush()
      }
    }

    val peerProtocol = new P2PRandomPeerSamplingProtocol(20, 0, 10, 1, true, true, PeerModeSelection.RAND, random.nextLong())
    val idleProtocol = new IdleProtocol() {
      override def compute[K: ClassTag](self: ProtocolVertexContext[K],
                                        messages: Seq[_ <: Message[K]],
                                        responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
                                        aggregator: Option[P2PRandomPeerSamplingProtocol#aggregatorType],
                                        superstep: Int)
      : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = {

        if (self.getId() == 1025) {
          self.accessProtocol(peerProtocol.name) match {
            case Some(cxt) =>
              var sample: Int = 0
              for (i <- 0 to 4) {
                val randomPeer = cxt.asInstanceOf[P2PRandomPeerSamplingVertexContext[K]].getPeer()
                randomPeer match {
                  case Some(peerId) => sample = sample | ((peerId.asInstanceOf[Int] - 1) << (10 * i))
                  case None => {}
                }
              }
              logInfo("Random sample: %s".format(sample))
            case None => logInfo("%s doesn't fetch any peer".format(self.getId))
          }
        }
        (false, new Array[Message[K]](0), new Array[RequestVertexContextMessage[K]](0))
      }
    }

    val onjag = new OnJag(sc, StorageLevel.MEMORY_ONLY,
      peerProtocol,
      idleProtocol)
    if (maxStep > 0)
      onjag.setMaxStep(maxStep)

    val numEdges = sc.accumulator(0)(IntAccumulatorParam)

    println("Parsing input file...")
    var vertices = input.map(line => {
      val fields = line.split("\t")
      val (id, body) = (fields(0).toInt, fields(1).replace("\\n", "\n"))
      val links = body.split(",").map(strLink => strLink.toInt).map(x => x.asInstanceOf[Any])
      numEdges += links.size
      val data = Array(
        links.asInstanceOf[Array[Any]],
        links.asInstanceOf[Array[Any]])
      (id, onjag.createVertex(id, data))
    })

    if (usePartitioner)
      vertices = vertices.partitionBy(new HashPartitioner(sc.defaultParallelism))

    println("Done parsing input file.")

    vertices.foreach(x => {}) // Force evaluation to compute the numEdges accumulator
    println("Done counting Edges. They are %d (%d undirected)".format(numEdges.value, numEdges.value / 2))

    //Print Initial Graph
    printInitialGraph(vertices)

    // Initialization
    val messages = onjag.createInitMessages(vertices, new Array[Any](2))

    val checkpointConditions = List(Some(new SuperstepIntervalCondition(1)), Some(new SuperstepIntervalCondition(1)))

    println("OnJag Execution Started")
    val result = onjag.run(sc, vertices, messages, checkpointConditions, numPartitions, PersistenceMode.DELETE_IMMEDIATELY_SYNCHED)
    println("OnJag Execution Ended")

    // Print the result
    printResultGraph(result)

    // Close the random sequence file
    file.close()

    val timeTaken = System.currentTimeMillis - startTime
    println(timeTaken)
  }

  def printInitialGraph[K](vertices: RDD[(K, Vertex[K])]) {
    val initGraph = (vertices.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(0).toString)
    }.collect.mkString)
    println("Init Graph PeerSampling")
    println(initGraph)
  }

  def printResultGraph[K](result: RDD[(K, Vertex[K])]) {
    val top = (result.map {
      case (id, vertex) =>
        "%s\n".format(vertex.protocolsContext(0).toString)
    }.collect.mkString)
    println("Final result PeerSampling")
    println(top)
  }
}
