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
package org.bdgenomics.mango.models

import net.liftweb.json.Serialization.write
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.mango.layout.BedRowJson
import org.bdgenomics.utils.misc.Logging

/**
 * Serves Keystone machine learning models that take (ReferenceRegion, sequence) as input
 *
 * @param reference Reference to fetch sequences from
 * @param filePaths Filepaths to serialized keystone machine learning objects
 */
class ModelMaterialization(reference: AnnotationMaterialization,
                           filePaths: List[String]) extends Serializable with Logging {

  @transient implicit val formats = net.liftweb.json.DefaultFormats
  val error = 0.01
  val models = filePaths.map(r => ModelServer(r))

  def getFiles = filePaths
  val getKeys = filePaths.map(f => LazyMaterialization.filterKeyFromFile(f))

  def get(region: ReferenceRegion): Map[String, String] = {

    // region is too large, return no valid data
    if (region.length > 5000) {
      println("region is too large")
      models.map(m => m.name).map(m => (m, "")).toMap
    } else {
      val in = (region, reference.getReferenceString(region))
      val flattened: Map[String, Array[BedRowJson]] =
        models.map(m => (m.name, m.serve(in).filter(_.getScore > error)))
          .map(r => (LazyMaterialization.filterKeyFromFile(r._1),
            r._2.map(f => BedRowJson(r._1, "model", f.getContigName, f.getStart, f.getEnd)))) // TODO: put in score for frontend
          .toMap
      flattened.mapValues(r => write(r))
    }

  }
}

// TODO: remove and replace with ENDIVE
case class ModelServer(filePath: String) {

  def name = filePath

  def serve(in: (ReferenceRegion, String)): Array[Feature] = {
    val region = in._1

    val regions = Array.range(0, in._1.length().toInt)
      .sliding(200, 50)
      .map(r => ReferenceRegion(region.referenceName, r.head + region.start, r.head + region.start + r.length))
      .toArray

    regions.map(r => {
      Feature.newBuilder()
        .setContigName(r.referenceName)
        .setStart(r.start)
        .setEnd(r.end)
        .setScore(1.0)
        .build()
    })
  }
}
