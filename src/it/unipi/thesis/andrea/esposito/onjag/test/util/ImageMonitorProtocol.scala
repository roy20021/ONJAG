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

package it.unipi.thesis.andrea.esposito.onjag.test.util

import it.unipi.thesis.andrea.esposito.onjag.core._
import org.apache.spark.{AccumulatorParam, Accumulator, SparkContext}
import scala.reflect.ClassTag
import it.unipi.thesis.andrea.esposito.onjag.protocols.{EuclideanSpaceVertexContext, IdleProtocol}

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 14/05/2014.
 */
class ImageMonitorProtocol(sampleStep: Int = 20) extends Protocol {
  override type aggregatorType = Nothing

  override var name: String = "imageMonitor"
  override val startStep: Int = 0

  override def step: Int = 1

  var sample: Accumulator[Seq[(Int, Int)]] = _

  private val accumulParam = new AccumulatorParam[Seq[(Int, Int)]]() {
    override def addInPlace(r1: Seq[(Int, Int)], r2: Seq[(Int, Int)])
    : Seq[(Int, Int)] = r1 ++ r2

    override def zero(initialValue: Seq[(Int, Int)]): Seq[(Int, Int)] = initialValue
  }

  override def init(sc: SparkContext): Unit = ()

  override def compute[K: ClassTag](self: ProtocolVertexContext[K],
                                    messages: Seq[_ <: Message[K]],
                                    responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
                                    aggregator: Option[IdleProtocol#aggregatorType],
                                    superstep: Int)
  : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = {
    val color = self.asInstanceOf[ImageMonitorVertexContext[K]].color
    self.accessProtocol("euclideantman") match {
      case Some(cxt) =>
        val tmanCxt = cxt.asInstanceOf[EuclideanSpaceVertexContext[K]]
        sample += tmanCxt.view.map(v => (self.getId().asInstanceOf[Int], v._1.asInstanceOf[Int]))
      case None => {}
    }
    (false, new Array[Message[K]](0), new Array[RequestVertexContextMessage[K]](0))
  }

  override def beforeSuperstep(sc: SparkContext, superstep: Int): Unit = {
    sample = sc.accumulator(Array[(Int, Int)]().toSeq)(accumulParam)
  }

  override def afterSuperstep(sc: SparkContext, superstep: Int): Unit = {
    if (superstep % sampleStep == 0) {
      println(lastSample())
    }
  }

  def lastSample(): String = sample.value.map(e => "%s,%s".format(e._1, e._2)).mkString("\n")

  override def createInitMessages[K: ClassTag](context: ProtocolVertexContext[K], data: Any)
  : (Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = (Seq[Message[K]](), Seq[RequestVertexContextMessage[K]]())

  override def brutalStoppable(): Boolean = false

  override def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any])
  : ProtocolVertexContext[K] = new ImageMonitorVertexContext[K](data(0).asInstanceOf[Array[Int]])
}

class ImageMonitorVertexContext[K](val color: Array[Int]) extends ProtocolVertexContext[K] {
  override def toString(): String = "%s,%s".format(getId(), color.map(c => "%s".format(c)).mkString(","))

}
