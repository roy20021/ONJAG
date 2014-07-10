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

import com.esotericsoftware.kryo.Kryo
import it.unipi.thesis.andrea.esposito.onjag.core._
import it.unipi.thesis.andrea.esposito.onjag.protocols._
import it.unipi.thesis.andrea.esposito.onjag.protocols.randompeersampling._

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 05/03/14.
 */
class TestKryoRegistrator extends OnJagKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)

    kryo.register(classOf[P2PRandomPeerSamplingProtocol])
    kryo.register(classOf[P2PRandomPeerSamplingMessage[_]])
    kryo.register(classOf[P2PRandomPeerSamplingVertexContext[_]])
    kryo.register(classOf[PeerModeSelection.Value])

    kryo.register(classOf[RandomPeerSamplingProtocol])
    kryo.register(classOf[RandomPeerSamplingVertexContext[_]])

    kryo.register(classOf[RangeRandomPeerSamplingProtocol])
    kryo.register(classOf[RangeRandomPeerSamplingVertexContext[_]])

    kryo.register(classOf[DummyProtocol])

    kryo.register(classOf[EuclideanSpaceTMANProtocol])
    kryo.register((classOf[EuclideanSpaceVertexContext[_]]))

    kryo.register(classOf[FindSubPartitionProtocol])
    kryo.register(classOf[FindSubPartitionBoostrapMessage[_]])
    kryo.register(classOf[FindSubPartitionMessage[_]])
    kryo.register(classOf[FindSubPartitionProtocolVertexContext[_]])

    kryo.register(classOf[IdleProtocol])

    kryo.register(classOf[JaBeJaProtocol])
    kryo.register(classOf[JaBeJaMessage[_]])
    kryo.register(classOf[JaBeJaVertexContext[_]])
    kryo.register(classOf[SendableJaBeJaVertexContext[_]])
    kryo.register(classOf[Header.Value])
    kryo.register(classOf[SWAP_TYPE.Value])
    kryo.register(classOf[PartnerPickOrder.Value])

    kryo.register(classOf[JaBeJaTMANProtocol])
    kryo.register(classOf[JaBeJaTMANVertexContext[_]])

    kryo.register(classOf[KCorenessProtocol])
    kryo.register(classOf[KCorenessMessage[_]])
    kryo.register(classOf[KCorenessVertexContext[_]])

    kryo.register(classOf[RingTMANProtocol])
    kryo.register(classOf[RingTMANVertexContext[_]])

    kryo.register(classOf[TMANProtocol])
    kryo.register(classOf[TMANVertexContext[_]])
    kryo.register(classOf[TMANMessage[_]])
  }
}
