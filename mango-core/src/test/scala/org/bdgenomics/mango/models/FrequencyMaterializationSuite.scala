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

import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
import org.bdgenomics.mango.util.MangoFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._
import net.liftweb.json._

class FrequencyMaterializationSuite extends MangoFunSuite {

  implicit val formats = DefaultFormats

  val chrM = resourcePath("mouse_chrM.bam")
  val chunkSize = 1000
  val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 2000L),
    SequenceRecord("chrM", 20000L),
    SequenceRecord("20", 90000L)))

  sparkTest("get frequency from file that is not precomputed") {
    val sample = "C57BL/6J"
    val region = ReferenceRegion("chrM", 1, 1000)
    val freq = new FrequencyMaterialization(sc, sd, chunkSize)
    freq.loadSample(chrM, sample)

    val results = freq.get(region, List(sample))
    val size = region.end - region.start + 1

  }

  sparkTest("validate frequency at specific points") {
    val sample = "C57BL/6J"
    val region = ReferenceRegion("chrM", 1, 10)
    val freq = new FrequencyMaterialization(sc, sd, chunkSize)
    freq.loadSample(chrM, sample)

    val results = freq.multiget(region, List(sample))

    val reads = sc.loadBam(chrM).rdd.filterByOverlappingRegion(region)

    val ones = reads.flatMap(r => (r.getStart.toLong to r.getEnd.toLong)).filter(_ == 1).count
    val threes = reads.flatMap(r => (r.getStart.toLong to r.getEnd.toLong)).filter(_ == 3).count

    val firstCoverage: SampleCoverage = parse(freq.multiget(ReferenceRegion("chrM", 1, 1), List(sample)).first).extract[SampleCoverage]
    val thirdCoverage: SampleCoverage = parse(freq.multiget(ReferenceRegion("chrM", 3, 3), List(sample)).first).extract[SampleCoverage]

    assert(firstCoverage.count == ones)
    assert(thirdCoverage.count == threes)

  }

}
