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

package it.unipi.thesis.andrea.esposito.onjag.protocols

import it.unipi.thesis.andrea.esposito.onjag.core._
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.SparkContext.IntAccumulatorParam
import scala.reflect.ClassTag

/**
 * Distributed k-Core Decomposition
 *
 * Paper:
 * Alberto Montresor, Francesco De Pellegrini, and Daniele Miorandi. 2011.
 * "Distributed k-core decomposition".
 * In Proceedings of the 30th annual ACM SIGACT-SIGOPS symposium on Principles of distributed computing (PODC '11).
 * ACM, New York, NY, USA, 207-208.
 */
class KCorenessProtocol(startStep_param: Int = 0, step_param: Int = 1) extends Protocol {
  var name: String = "kcoreness"
  val startStep: Int = startStep_param
  type aggregatorType = Nothing

  def step: Int = step_param

  var avgMsgsSent = 0.0
  var maxMsgsSent = Double.MinValue
  var msgsSentCounter: Accumulator[Int] = _

  /**
   * Initializer of the Protocol.
   * Invoked the first time an [[OnJag]] computation starts.
   * @param sc
   */
  override def init(sc: SparkContext): Unit = {
    msgsSentCounter = sc.accumulator(0)
  }

  def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): KCorenessVertexContext[K] = {
    val context = new KCorenessVertexContext[K](data.length)
    for (i <- 0 until data.length) {
      context.links(i) = (data(i).asInstanceOf[K], Int.MaxValue)
    }
    context
  }

  def createInitMessages[K: ClassTag](context: ProtocolVertexContext[K], data: Any)
  : (Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = {
    val kcoreContext = context.asInstanceOf[KCorenessVertexContext[K]]
    val msgs = kcoreContext.links.map {
      l =>
        val targetId = l._1
        new KCorenessMessage[K](kcoreContext.getId, name, targetId, name, kcoreContext.kcoreness)
    }
    (msgs, new Array[RequestVertexContextMessage[K]](0))
  }

  def compute[K: ClassTag](self: ProtocolVertexContext[K],
                           messages: Seq[_ <: Message[K]],
                           responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
                           aggregator: Option[KCorenessProtocol#aggregatorType],
                           superstep: Int)
  : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = {
    val kcoreContext = self.asInstanceOf[KCorenessVertexContext[K]]

    val (changed, kcoreness, links) = receive(kcoreContext, messages)

    val active = changed
    var newMessages = new Array[Message[K]](0)
    kcoreContext.kcoreness = kcoreness
    kcoreContext.links = links
    if (changed) {
      newMessages = kcoreContext.links.map {
        case l =>
          val targetId = l._1
          new KCorenessMessage[K](kcoreContext.getId, name, targetId, name, kcoreContext.kcoreness)
      }
    }

    msgsSentCounter += newMessages.size

    (!active, newMessages, new Array[RequestVertexContextMessage[K]](0))
  }

  private def receive[K: ClassTag](self: KCorenessVertexContext[K], messages: Seq[_ <: Message[K]]): (Boolean, Int, Array[(K, Int)]) = {
    val (changed, kcoreness, links) = messages.foldLeft((false, self.kcoreness, self.links.clone)) {
      case (prec, message) =>
        var changed = prec._1
        var kcoreness = prec._2
        val links = prec._3
        val index = links.indexWhere(link => link._1 == message.sourceId)
        val kcorenessReceived = message.asInstanceOf[KCorenessMessage[K]].kcoreness
        val kcoreStored = links(index)
        if (kcorenessReceived < kcoreStored._2) {
          links(index) = (kcoreStored._1, kcorenessReceived)
          val t = computeIndex(kcoreness, links)
          if (t < kcoreness) {
            kcoreness = t
            changed = true
          }
        }
        (changed, kcoreness, links)
    }
    (changed, kcoreness, links)
  }

  private def computeIndex[K: ClassTag](kcoreness: KCorenessVertexContext[K]#kcorenessType, links: Array[(K, KCorenessVertexContext[K]#kcorenessType)]): Int = {
    val count: Array[Int] = new Array[Int](kcoreness).map(value => 0: Int)

    for (link <- links) {
      val j = scala.math.min(kcoreness, link._2) - 1
      count(j) = count(j) + 1
    }

    for (i <- kcoreness - 1 to 1 by -1) {
      count(i - 1) = count(i - 1) + count(i)
    }

    var i = kcoreness
    while (i > 1 && count(i - 1) < i)
      i = i - 1

    i
  }

  def brutalStoppable(): Boolean = false

  def afterSuperstep(sc: SparkContext, superstep: Int): Unit = {
    avgMsgsSent = ((avgMsgsSent * superstep - 1) + msgsSentCounter.value) / superstep.toDouble
    if (msgsSentCounter.value > maxMsgsSent)
      maxMsgsSent = msgsSentCounter.value

    msgsSentCounter = sc.accumulator(0)
  }

  def beforeSuperstep(sc: SparkContext, superstep: Int): Unit = ()
}

class KCorenessMessage[K](val sourceId: K,
                          val sourceProtocol: String,
                          val targetId: K,
                          val targetProtocol: String,
                          val kcoreness: KCorenessVertexContext[K]#kcorenessType)
  extends Message[K] {}

class KCorenessVertexContext[K](var kcoreness: KCorenessVertexContext[K]#kcorenessType) extends ProtocolVertexContext[K] {

  type kcorenessType = Int

  var links: Array[(K, kcorenessType)] = new Array[(K, kcorenessType)](kcoreness)
}