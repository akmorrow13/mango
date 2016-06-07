/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.mango.tiling

import org.apache.spark.Logging
import org.bdgenomics.adam.models.ReferenceRegion

import scala.reflect.ClassTag

abstract class NLayeredTile[S: ClassTag] extends Serializable with Logging {
  def rawData: Array[S]
  def layerMap: Map[Int, Array[S]]

  def getAggregated(region: ReferenceRegion): Array[S] = {
    val size = region.length()

    size match {
      case x if (x < L1.range._1) => throw new Exception(s"Should fetch raw data for regions < ${L1.range._1}")
      case x if (x >= L1.range._1 && x < L1.range._2) => layerMap(1)
      case x if (x >= L2.range._1 && x < L2.range._2) => layerMap(2)
      case x if (x >= L3.range._1 && x < L3.range._2) => layerMap(3)
      case _ => layerMap(4)
    }
  }

}

object LayeredTile extends Serializable {

  val layerCount = 5
  val layers = Map(1 -> L1, 2 -> L2, 3 -> L3, 4 -> L4)

  def getLayer(region: ReferenceRegion): Layer = {
    val size = region.length()
    size match {
      case x if (x < L1.range._1) => L0
      case x if (x >= L1.range._1 && x < L1.range._2) => L1
      case x if (x >= L2.range._1 && x < L2.range._2) => L2
      case x if (x >= L3.range._1 && x < L3.range._2) => L3
      case _ => L4
    }
  }
}

trait Layer extends Serializable {
  def id: Int
  def maxSize: Long
  def range: Tuple2[Long, Long]
  val finalSize = 1000

  def patchSize: Int
  def stride: Int

  def fromDoubleBytes(arr: Array[Byte]): Array[Double] = arr.map(_.toDouble)

}

/* For raw data */
object L0 extends Layer {
  val id = 0
  var maxSize = 5000L
  var range = (0L, maxSize)
  var patchSize = 0
  var stride = 0

  def fromCharBytes(arr: Array[Byte]): String = arr.map(_.toChar).mkString
}

/* For objects 5000 to 10000 */
object L1 extends Layer {
  val id = 1
  var maxSize = 10000L
  var range = (5000L, maxSize)
  var patchSize = 10
  var stride = 10
}

/* For objects 10,000 to 100,000 */
object L2 extends Layer {
  val id = 2
  var maxSize = 100000L
  var range = (L1.maxSize, maxSize)
  var patchSize = 100
  var stride = patchSize
}

/* For objects 100,000 to 1,000,000 */
object L3 extends Layer {
  val id = 3
  var maxSize = 1000000L
  var range = (L2.maxSize, maxSize)
  var patchSize = 1000
  var stride = patchSize
}

/* For objects 1000000 + */
object L4 extends Layer {
  val id = 4
  var maxSize = 10000000L
  var range = (L3.maxSize, maxSize)
  var patchSize = 10000
  var stride = patchSize
}