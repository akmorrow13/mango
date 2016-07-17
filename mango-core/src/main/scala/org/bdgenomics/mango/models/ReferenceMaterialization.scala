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

import java.io.File

import htsjdk.samtools.SAMSequenceDictionary
import net.liftweb.json.Serialization._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{ ReferenceContigMap, ReferenceFile, TwoBitFile }
import org.bdgenomics.formats.avro.{ Feature, NucleotideContigFragment }
import org.bdgenomics.mango.core.util.ResourceUtils
import org.bdgenomics.mango.layout.GeneJson
import org.bdgenomics.utils.intervalrdd.IntervalRDD
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.bdgenomics.utils.misc.Logging
import picard.sam.CreateSequenceDictionary

class ReferenceMaterialization(@transient sc: SparkContext,
                               referencePath: String, genePath: Option[String] = None) extends Serializable with Logging {

  @transient implicit val formats = net.liftweb.json.DefaultFormats
  var bookkeep = Array[String]()

  // set and name interval rdd
  val (referenceFile: ReferenceFile, dict: SequenceDictionary) =
    if (referencePath.endsWith(".fa") || referencePath.endsWith(".fasta")) {
      val createObj = new CreateSequenceDictionary
      val dict: SAMSequenceDictionary = createObj.makeSequenceDictionary(new File(referencePath))
      (sc.loadSequences(referencePath, fragmentLength = 10000), SequenceDictionary(dict))
    } else if (referencePath.endsWith(".2bit")) {
      val twoBit = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
      (twoBit, twoBit.getSequenceDictionary())
    } else if (referencePath.endsWith(".adam")) {
      val reference = sc.loadParquetContigFragments(referencePath)
      (reference, reference.getSequenceDictionary())
    } else
      throw new UnsupportedFileException("File Types supported for reference are fa, fasta and adam")

  var hasGenes: Boolean = false
  val geneRDD: IntervalRDD[ReferenceRegion, Gene] = loadGenes(genePath)
  geneRDD.setName("Gene RDD")

  /**
   *
   * @param filePath: bed or ADAM feature filepath
   * @return RDD of features to be formatted as genes
   */
  def loadGenes(filePath: Option[String]): IntervalRDD[ReferenceRegion, Gene] = {
    filePath match {
      case Some(_) => {
        if (filePath.get.endsWith(".gtf") && filePath.get.endsWith(".adam")) {
          throw new UnsupportedFileException(s"${filePath.get} not supported for genes")
        }
        val features: RDD[Feature] = FeatureMaterialization.load(sc, None, filePath.get)
        val fixedParentIds: RDD[Feature] = features.reassignParentIds

        val genes: RDD[(ReferenceRegion, Gene)] = fixedParentIds.toGenes.map(g => {
          (ReferenceRegion(g.regions.head.referenceName, g.regions.map(_.start).min, g.regions.map(_.end).max), g)
        })
        if (!genes.isEmpty())
          hasGenes = true
        IntervalRDD(genes)
      }
      case None => IntervalRDD(sc.emptyRDD[(ReferenceRegion, Gene)])
    }
  }

  def getGenes(region: ReferenceRegion): String = {
    val genes = geneRDD.filterByInterval(region).toRDD
    val json = genes.flatMap(g => GeneJson(g)).collect

    write(json)
  }

  def getSequenceDictionary: SequenceDictionary = dict

  def getReferenceString(region: ReferenceRegion): String = {
    referenceFile.extract(region).toUpperCase()
  }
}