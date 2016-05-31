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

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.mango.layout._

case class AlignmentRecordTile(data: Iterable[AlignmentRecord],
                               reference: String,
                               paddedRegion: ReferenceRegion) extends KLayeredTile with Serializable {

  // raw data is alignments and mismatches
  lazy val rawData = data.map(r => CalculatedAlignmentRecord(r, MismatchLayout(r, reference, paddedRegion)))
    .filter(r => !r.mismatches.isEmpty).groupBy(_.record.getRecordGroupSample)

  // layer 1 is point mismatches
  lazy val layer1 = rawData.mapValues(rs => PointMisMatch(rs.flatMap(_.mismatches).toList))

  val layerMap: Map[Int, Map[String, Iterable[Any]]] = Map(0 -> rawData, 1 -> layer1)

}

case class ReferenceTile(sequence: String) extends LayeredTile[String] with Serializable {
  val rawData = sequence
  val layerMap = null
}