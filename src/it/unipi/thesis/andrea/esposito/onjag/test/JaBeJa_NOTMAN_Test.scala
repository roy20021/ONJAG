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

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 21/01/14.
 */
object JaBeJa_NOTMAN_Test {

  def main(args: Array[String]) {
    if (args.length < 18) {
      System.err.println("Usage: app <inputFile> <usePartitioner> <numPartitionsSpark> <maxStep> <host> <spark_home> <checkpointDir> <numPartitionJaBeJa> <seedColoring> <seedProtocol> <alpha_jabeja> <start_SA> <delta_SA> <randomViewSize> <partnerPickOrder> <partner (prob1,prob2,..,probN)> <minCutSamplingRate> <approximate>")
      System.exit(-1)
    }

    JaBeJa_Test.main(args :+ false.toString)
  }
}
