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

import java.io.{ BufferedOutputStream, OutputStream, PrintWriter, StringWriter }

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.VariantContextBuilder
import net.liftweb.json.Serialization.write
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ VariantContext, ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.projections.{ Projection, VariantField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.{ VariantRDD, GenotypeRDD, VariantContextRDD }
import org.bdgenomics.convert.ConversionStringency
import org.bdgenomics.formats.avro.{ Variant, GenotypeAllele }
import org.bdgenomics.mango.converters.GA4GHutil
import org.bdgenomics.mango.core.util.ResourceUtils
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import org.bdgenomics.mango.converters.GA4GHutil._

/**
 * Handles loading and tracking of data from persistent storage into memory for Variant data.
 *
 * @param sc SparkContext
 * @param files list files to materialize
 * @param sd the sequence dictionary associated with the file records
 * @param repartition whether to repartition data to the default number of partitions
 * @param prefetchSize the number of base pairs to prefetch in executors. Defaults to 1000000
 * @see LazyMaterialization.scala
 */
class VariantContextMaterialization(@transient sc: SparkContext,
                                    files: List[String],
                                    sd: SequenceDictionary,
                                    repartition: Boolean = false,
                                    prefetchSize: Option[Long] = None)
    extends LazyMaterialization[VariantContext, ga4gh.Variants.Variant](VariantContextMaterialization.name, sc, files, sd, repartition, prefetchSize)
    with Serializable {

  // placeholder used for ref/alt positions to display in browser
  val variantPlaceholder = "N"

  /**
   * Extracts ReferenceRegion from Variant
   *
   * @return extracted ReferenceRegion
   */
  def getReferenceRegion = (g: VariantContext) => ReferenceRegion(g.position)

  /**
   * Reset ReferenceName for Feature
   *
   * @param f VariantContext to be modified
   * @param contig to replace Feature contigName
   * @return Feature with new ReferenceRegion
   */
  def setContigName = (f: VariantContext, contig: String) => {
    val variant = Variant.newBuilder(f.variant.variant)
      .setContigName(contig).build()

    VariantContext(variant, f.genotypes)
  }

  /**
   * Loads VariantContext Data
   *
   * @return Generic RDD of data types from file
   */
  def load = (file: String, regions: Option[Iterable[ReferenceRegion]]) =>
    VariantContextMaterialization.load(sc, file, regions).rdd

  /**
   * TODO what about genotypes?
   * Stringifies data from variants to lists of variants over the requested regions
   *
   * @param data RDD of  filtered (key, GenotypeJson)
   * @return Map of (key, json) for the ReferenceRegion specified
   *
   */
  def toJson(data: RDD[(String, VariantContext)]): Map[String, Array[ga4gh.Variants.Variant]] = {
    data
      .collect.groupBy(_._1).mapValues(r =>
        {
          r.map(a => variantContextToGAVariant(a._2))
        })
  }

  /**
   * Formats raw data from GA4GH Variants Response to JSON.
   * @param data An array of GA4GH Variants
   * @return JSONified data
   */
  def stringify = (data: Array[ga4gh.Variants.Variant]) => {
    // write message
    val message = ga4gh.VariantServiceOuterClass
      .SearchVariantsResponse.newBuilder().addAllVariants(data.toList)
      .build()

    // get message size
    val pblen: Int = message.getSerializedSize

    // make output buffer to write to
    val buf = new Array[Byte](pblen)
    val out = com.google.protobuf.CodedOutputStream.newInstance(buf)

    // write message to buffer
    message.writeTo(out)

    buf
  }

  /**
   * Gets all SampleIds for all genotypes in each file. If no genotypes are available, will return an empty sequence.
   *
   * @return List of filenames their corresponding Seq of SampleIds.
   */
  def getGenotypeSamples(): List[(String, List[String])] = {
    files.map(fp => (fp, VariantContextMaterialization.load(sc, fp, None).samples.map(_.getSampleId).toList))
  }
}

/**
 * VariantContextMaterialization object, used to load VariantContext data into a VariantContextRDD. Supported file
 * formats are vcf and adam.
 */
object VariantContextMaterialization {

  val name = "VariantContext"
  val datasetCache = new collection.mutable.HashMap[String, GenotypeRDD]

  /**
   * Loads variant data from adam and vcf files into a VariantContextRDD
   *
   * @param sc SparkContext
   * @param fp filePath to load
   * @param regions Iterable of ReferenceRegions to predicate load
   * @return VariantContextRDD
   */
  def load(sc: SparkContext, fp: String, regions: Option[Iterable[ReferenceRegion]]): VariantContextRDD = {
    if (fp.endsWith(".adam")) {
      loadAdam(sc, fp, regions)
    } else {
      try {
        loadVariantContext(sc, fp, regions)
      } catch {
        case e: Exception => {
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          throw UnsupportedFileException("File type not supported. Stack trace: " + sw.toString)
        }
      }
    }
  }

  /**
   * Loads VariantContextRDD from a vcf file. vcf tbi index is required.
   *
   * @param sc SparkContext
   * @param fp filePath to vcf file
   * @param regions Iterable of ReferencesRegion to predicate load
   * @return VariantContextRDD
   */
  def loadVariantContext(sc: SparkContext, fp: String, regions: Option[Iterable[ReferenceRegion]]): VariantContextRDD = {
    if (regions.isDefined) {
      val predicateRegions: Iterable[ReferenceRegion] = regions.get
        .flatMap(r => LazyMaterialization.getContigPredicate(r))
      sc.loadIndexedVcf(fp, predicateRegions)
    } else {
      sc.loadVcf(fp)
    }
  }

  /**
   * Loads adam variant files
   *
   * @param sc SparkContext
   * @param fp filePath to load variants from
   * @param regions Iterable of  ReferenceRegions to predicate load
   * @return VariantContextRDD
   */
  def loadAdam(sc: SparkContext, fp: String, regions: Option[Iterable[ReferenceRegion]]): VariantContextRDD = {

    val variantContext = if (sc.isPartitioned(fp)) {

      // finalRegions includes contigs both with and without "chr" prefix
      val finalRegions: Iterable[ReferenceRegion] = regions.get ++ regions.get
        .map(x => ReferenceRegion(x.referenceName.replaceFirst("""^chr""", """"""),
          x.start,
          x.end,
          x.strand))

      // load new dataset or retrieve from cache
      val data: GenotypeRDD = datasetCache.get(fp) match {
        case Some(ds) => { // if dataset found in datasetCache
          ds
        }
        case _ => {
          // load dataset into cache and use use it
          datasetCache(fp) = sc.loadPartitionedParquetGenotypes(fp)
          datasetCache(fp)
        }
      }

      val maybeFiltered: GenotypeRDD = if (finalRegions.nonEmpty) {
        data.filterByOverlappingRegions(finalRegions)
      } else data

      maybeFiltered.toVariantContexts()

    } else {
      val pred =
        if (regions.isDefined) {
          val prefixRegions: Iterable[ReferenceRegion] = regions.get.map(r => LazyMaterialization.getContigPredicate(r)).flatten
          Some(ResourceUtils.formReferenceRegionPredicate(prefixRegions))
        } else {
          None
        }
      sc.loadParquetGenotypes(fp, optPredicate = pred).toVariantContexts()

    }
    variantContext
  }

  //  /**
  //   * Converts VariantContextRDD into RDD of Variants and Genotype SampleIds that can be directly converted to json
  //   *
  //   * @param v VariantContextRDD to Convert
  //   * @return Converted json RDD
  //   */
  //  private def toGenotypeJsonRDD(v: VariantContextRDD): RDD[GenotypeJson] = {
  //    v.rdd.map(r => {
  //      // filter out genotypes with only some alt alleles
  //      val genotypes = r.genotypes.filter(_.getAlleles.toArray.filter(_ != GenotypeAllele.REF).length > 0)
  //      new GenotypeJson(r.variant.variant, genotypes.map(_.getSampleId).toArray)
  //    })
  //  }
}
