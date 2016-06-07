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

import edu.berkeley.cs.amplab.spark.intervalrdd.IntervalRDD
import net.liftweb.json.Serialization.write
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.projections.{ GenotypeField, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.mango.layout.{ VariantFreq, VariantLayout }
import org.bdgenomics.mango.tiling.{ KTiles, VariantTile }
import org.bdgenomics.mango.util.Bookkeep

import scala.reflect.ClassTag

/*
 * Handles loading and tracking of data from persistent storage into memory for Genotype data.
 * @see LazyMaterialization.scala
 */
class GenotypeMaterialization(s: SparkContext, d: SequenceDictionary, parts: Int, chunkS: Int) extends LazyMaterialization[Genotype, VariantTile]
    with KTiles[VariantTile] with Serializable with Logging {

  val sc = s
  val dict = d
  val partitions = parts
  val chunkSize = chunkS
  val partitioner = setPartitioner
  val bookkeep = new Bookkeep(chunkSize)

  def loadAdam(region: ReferenceRegion, fp: String): RDD[Genotype] = {
    val pred: FilterPredicate = ((LongColumn("end") >= region.start) && (LongColumn("start") <= region.end) && (BinaryColumn("contigName") === (region.referenceName)))
    val proj = Projection(GenotypeField.start, GenotypeField.end, GenotypeField.alleles, GenotypeField.sampleId)
    sc.loadParquetGenotypes(fp, predicate = Some(pred), projection = Some(proj))
    val genes = sc.loadParquetGenotypes(fp, predicate = Some(pred))
    genes
  }

  /**
   * Stringifies raw genotypes to a string
   * @param rdd
   * @param region
   * @return
   */
  def stringifyL0(rdd: RDD[(String, Iterable[Any])], region: ReferenceRegion): String = {
    implicit val formats = net.liftweb.json.DefaultFormats

    val data: Array[(String, Iterable[Genotype])] = rdd
      .mapValues(_.asInstanceOf[Iterable[Genotype]])
      .mapValues(r => r.filter(r => r.getStart <= region.end && r.getEnd >= region.start)).collect

    val flattened: Map[String, Array[Genotype]] = data.groupBy(_._1)
      .map(r => (r._1, r._2.flatMap(_._2)))

    // write map of (key, data)
    write(flattened.mapValues(r => VariantLayout(r)))
  }

  /**
   * Stringifies raw genotypes to a string
   * @param rdd
   * @param region
   * @return
   */
  def stringifyL1(rdd: RDD[(String, Iterable[Any])], region: ReferenceRegion): String = {
    implicit val formats = net.liftweb.json.DefaultFormats

    val data: Array[(String, Iterable[VariantFreq])] = rdd
      .mapValues(_.asInstanceOf[Iterable[VariantFreq]])
      .mapValues(r => r.filter(r => r.start >= region.start && r.start <= region.end)).collect

    val flattened: Map[String, Array[VariantFreq]] = data.groupBy(_._1)
      .map(r => (r._1, r._2.flatMap(_._2)))

    // write map of (key, data)
    write(flattened)
  }

  override def getFileReference(fp: String): String = {
    fp
  }

  def loadFromFile(region: ReferenceRegion, k: String): RDD[Genotype] = {
    if (!fileMap.containsKey(k)) {
      log.error("Key not in FileMap")
      null
    }
    val fp = fileMap(k)
    if (fp.endsWith(".adam")) {
      println("LOADING FROM ADAM")
      loadAdam(region, fp)
    } else if (fp.endsWith(".vcf")) {
      sc.loadGenotypes(fp).filterByOverlappingRegion(region)
    } else {
      throw UnsupportedFileException("File type not supported")
      null
    }
  }

  /**
   *  Transparent to the user, should only be called by get if IntervalRDD.get does not return data
   * Fetches the data from disk, using predicates and range filtering
   * Then puts fetched data in the IntervalRDD, and calls multiget again, now with the data existing
   *
   * @param region ReferenceRegion in which data is retreived
   * @param ks to be retreived
   */
  override def put(region: ReferenceRegion, ks: List[String]) = {
    val seqRecord = dict(region.referenceName)
    seqRecord match {
      case Some(_) =>
        val end =
          Math.min(region.end, seqRecord.get.length)
        val start = Math.min(region.start, end)
        val reg = new ReferenceRegion(region.referenceName, start, end)
        ks.map(k => {
          val data = loadFromFile(reg, k)
          //TODO: add partitioner?
          println(reg)
          // TODO: remove hard coded key
          val tiles = Array((region, VariantTile(data.collect.toIterable, 0)))
          if (intRDD == null) {
            intRDD = IntervalRDD(sc.parallelize(tiles))
            intRDD.persist(StorageLevel.MEMORY_AND_DISK)
          } else {
            intRDD = intRDD.multiput(tiles)
            intRDD.persist(StorageLevel.MEMORY_AND_DISK)
          }
        })
        bookkeep.rememberValues(region, ks)
      case None =>
    }
  }
}

object GenotypeMaterialization {

  def apply(sc: SparkContext, dict: SequenceDictionary, partitions: Int): GenotypeMaterialization = {
    new GenotypeMaterialization(sc, dict, partitions, 100)
  }

  def apply[T: ClassTag, C: ClassTag](sc: SparkContext, dict: SequenceDictionary, partitions: Int, chunkSize: Int): GenotypeMaterialization = {
    new GenotypeMaterialization(sc, dict, partitions, chunkSize)
  }
}