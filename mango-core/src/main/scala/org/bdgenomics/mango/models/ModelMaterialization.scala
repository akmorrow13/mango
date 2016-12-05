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
import net.fnothaft.fig.models.Motif

import net.liftweb.json.Serialization.write
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.mango.layout.BedRowJson
import org.bdgenomics.mango.util.Bookkeep
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.intervalarray.IntervalArray
import org.bdgenomics.utils.misc.Logging
import net.akmorrow13.endive.pipelines.ModelServer

/**
 * Serves Keystone machine learning models that take (ReferenceRegion, sequence) as input
 *
 * @param reference Reference to fetch sequences from
 * @param filePaths Filepaths to serialized keystone machine learning objects
 */
class ModelMaterialization(reference: AnnotationMaterialization,
                           filePaths: List[String]) extends Serializable with Logging {

  val bookkeep: Bookkeep = new Bookkeep(2000)
  var cache: IntervalArray[ReferenceRegion, List[BedRowJson]] =
    new IntervalArray(Array.empty[(ReferenceRegion, List[BedRowJson])], 200)

  @transient implicit val formats = net.liftweb.json.DefaultFormats
  val error = 0.01
  val models = filePaths.map(r => ModelServer(r))

  def getFiles = filePaths
  val getKeys = filePaths.map(f => LazyMaterialization.filterKeyFromFile(f))

  def get(region: ReferenceRegion): Map[String, String] = {

    // check cache to see if values were already calculated
    val regionsOpt = bookkeep.getMissingRegions(region, filePaths)
    if (regionsOpt.isDefined) {
      for (r <- regionsOpt.get) {
        put(r)
      }
    }
    cache.get(region).map(_._2).flatten
      .groupBy(_.id)
      .map(r => (r._1, write(r._2)))
  }

  def put(region: ReferenceRegion) = {

    // drop values if new chr is read in
    if (!bookkeep.queue.contains(region.referenceName)) {
      try {
        val dropped = bookkeep.dropValues() // drop last chr
        cache = cache.filter(r => r._1.referenceName != dropped)
      }
    }

    ModelTimers.ModelServerRequest.time {
      // region is too large, return no valid data
      if (region.length > 10000) {
        log.warn("region is too large")
        models.map(m => m.name).map(m => (m, "")).toMap
      } else {
        val in =
          ModelTimers.GetSequenceRequest.time {
            (region, reference.getReferenceString(region))
          }
        println(in)
        val flattened: List[BedRowJson] =
          models.map(m => (m.name, m.serve(in).filter(_.getScore > error)))
            .flatMap(r => r._2.map(f => BedRowJson(LazyMaterialization.filterKeyFromFile(r._1), "model",
              f.getContigName, f.getStart, f.getEnd))) // TODO: put in score for frontend

        println(flattened)
        // insert elements into cache
        cache = cache.insert(Iterator((region, flattened)))

        // remember this region
        bookkeep.rememberValues(region, filePaths)
      }
    }
  }

}

object ModelTimers extends Metrics {
  val ModelServerRequest = timer("serve model")
  val GetSequenceRequest = timer("get sequence for model")
}
