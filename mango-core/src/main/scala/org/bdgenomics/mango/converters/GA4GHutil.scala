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
package org.bdgenomics.mango.converters

import javax.inject.Inject

import com.google.inject._
import ga4gh.ReadServiceOuterClass.SearchReadsResponse
import ga4gh.Reads.ReadAlignment
import ga4gh.SequenceAnnotationServiceOuterClass.SearchFeaturesResponse
import ga4gh.VariantServiceOuterClass.SearchVariantsResponse
import net.codingwell.scalaguice.ScalaModule
import net.liftweb.json.Extraction._
import net.liftweb.json._
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variant.{ GenotypeRDD, VariantContextRDD, VariantRDD }
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.convert.ga4gh.Ga4ghModule
import org.bdgenomics.convert.{ ConversionStringency, Converter }
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory

/*
 * Converts ADAM RDDs to json strings in GA4GH format.
 * See https://github.com/ga4gh/ga4gh-schemas.
 */
object GA4GHutil {
  val injector: Injector = Guice.createInjector(new Ga4ghModule())

  val alignmentConverter: Converter[AlignmentRecord, ReadAlignment] = injector
    .getInstance(Key.get(new TypeLiteral[Converter[AlignmentRecord, ReadAlignment]]() {}))

  val variantConverter: Converter[org.bdgenomics.formats.avro.Variant, ga4gh.Variants.Variant] = injector
    .getInstance(Key.get(new TypeLiteral[Converter[org.bdgenomics.formats.avro.Variant, ga4gh.Variants.Variant]]() {}))

  val genotypeConverter: Converter[org.bdgenomics.formats.avro.Genotype, ga4gh.Variants.Call] = injector
    .getInstance(Key.get(new TypeLiteral[Converter[org.bdgenomics.formats.avro.Genotype, ga4gh.Variants.Call]]() {}))

  val featureConverter: Converter[org.bdgenomics.formats.avro.Feature, ga4gh.SequenceAnnotations.Feature] = injector
    .getInstance(Key.get(new TypeLiteral[Converter[org.bdgenomics.formats.avro.Feature, ga4gh.SequenceAnnotations.Feature]]() {}))

  val logger = LoggerFactory.getLogger("GA4GHutil")

  /**
   * Converts avro Alignment Record to GA4GH Read Alignment
   *
   * @param alignmentRecord Alignment Record
   * @return GA4GH Read Alignment
   */
  def alignmentRecordToGAReadAlignment(alignmentRecord: AlignmentRecord): ReadAlignment = {
    alignmentConverter.convert(alignmentRecord, ConversionStringency.LENIENT, logger)
  }

  /**
   * Converts VariantContext to GA4GH Variant
   *
   * @param variantContext variant context
   * @return GA4GH Variant
   */
  def variantContextToGAVariant(variantContext: VariantContext) = {
    ga4gh.Variants.Variant.newBuilder(variantConverter.convert(variantContext.variant.variant, ConversionStringency.LENIENT, logger))
      .addAllCalls(variantContext.genotypes.map((g) => genotypeConverter.convert(g, ConversionStringency.LENIENT, logger)).asJava)
      .build()
  }

  /**
   * Converts avro features to GA4GH features
   *
   * @param feature avro Features
   * @return GA4GH Feature
   */
  def featureToGAFeature(feature: Feature): ga4gh.SequenceAnnotations.Feature = {
    ga4gh.SequenceAnnotations.Feature
      .newBuilder(featureConverter.convert(feature, ConversionStringency.LENIENT, logger)).build()
  }

  /**
   * Converts a JSON formatted ga4gh.VariantServiceOuterClass.  in string form
   * back into a SearchVariantsResponse.
   *
   * @param variantServiceString string of JSONified
   * @return converted SearchVariantsResponse
   */
  def stringToVariantServiceResponse(variantServiceString: String): SearchVariantsResponse = {

    val builder = ga4gh.VariantServiceOuterClass.SearchVariantsResponse.newBuilder()

    com.google.protobuf.util.JsonFormat.parser().merge(variantServiceString,
      builder)

    builder.build()
  }

  /**
   * Converts JSON formatted ga4gh.ReadServiceOuterClass.SearchReadsResponse in string form
   * back into a SearchReadsResponse.
   *
   * @param readsServiceString string of JSONified ga4gh.ReadServiceOuterClass.SearchReadsResponse
   * @return converted SearchReadsResponse
   */
  def stringToSearchReadsResponse(readsServiceString: String): SearchReadsResponse = {

    val builder = SearchReadsResponse.newBuilder()

    com.google.protobuf.util.JsonFormat.parser().merge(readsServiceString,
      builder)

    builder.build()
  }

  /**
   * Converts a JSON formatted ga4gh.SequenceAnnotationServiceOuterClass.SearchFeaturesResponse
   * in string form back into a SearchFeaturesResponse.
   *
   * @param featureServiceString string of JSONified ga4gh.SequenceAnnotationServiceOuterClass.SearchFeaturesResponse
   * @return converted SearchFeaturesResponse
   */
  def stringToSearchFeaturesResponse(featureServiceString: String): SearchFeaturesResponse = {

    val builder = SearchFeaturesResponse.newBuilder()

    com.google.protobuf.util.JsonFormat.parser().merge(featureServiceString,
      builder)

    builder.build()
  }

  /**
   * Converts alignmentRecordRDD to GA4GHReadsResponse string
   *
   * @param alignmentRecordRDD rdd to convert
   * @return GA4GHReadsResponse json string
   */
  def alignmentRecordRDDtoJSON(alignmentRecordRDD: AlignmentRecordRDD): String = {

    val gaReads: Array[ReadAlignment] = alignmentRecordRDD.rdd.collect.map(a => alignmentConverter.convert(a, ConversionStringency.LENIENT, logger))

    val result: ga4gh.ReadServiceOuterClass.SearchReadsResponse = ga4gh.ReadServiceOuterClass.SearchReadsResponse.newBuilder()
      .addAllAlignments(gaReads.toList.asJava).build()

    com.google.protobuf.util.JsonFormat.printer().includingDefaultValueFields().print(result)
  }

  /**
   * Converts variantContextRDD to GA4GHVariantResponse
   *
   * @param variantContextRDD rdd to convert
   * @return GA4GHVariantResponse json string
   */
  def variantContextRDDtoJSON(variantContextRDD: VariantContextRDD): String = {

    val gaVariants: Array[ga4gh.Variants.Variant] = variantContextRDD.rdd.collect.map(a => {
      variantContextToGAVariant(a)
    })

    val result: ga4gh.VariantServiceOuterClass.SearchVariantsResponse = ga4gh.VariantServiceOuterClass
      .SearchVariantsResponse.newBuilder().addAllVariants(gaVariants.toList.asJava).build()

    com.google.protobuf.util.JsonFormat.printer().includingDefaultValueFields().print(result)
  }

  /**
   * Converts genotypeRDD to GA4GHVariantResponse
   *
   * @param genotypeRDD rdd to convert
   * @return GA4GHVariantResponse json string
   */
  def genotypeRDDtoJSON(genotypeRDD: GenotypeRDD): String = {
    variantContextRDDtoJSON(genotypeRDD.toVariantContexts)
  }

  def featureRDDtoJSON(featureRDD: FeatureRDD): String = {

    val gaFeatures: Array[ga4gh.SequenceAnnotations.Feature] = featureRDD.rdd.collect.map(a => featureToGAFeature(a))

    val result: ga4gh.SequenceAnnotationServiceOuterClass.SearchFeaturesResponse = ga4gh.SequenceAnnotationServiceOuterClass.SearchFeaturesResponse
      .newBuilder().addAllFeatures(gaFeatures.toList.asJava).build()

    com.google.protobuf.util.JsonFormat.printer().includingDefaultValueFields().print(result)

  }

  /**
   * Converts variantRDD to GA4GHVariantResponse
   *
   * @param variantRDD rdd to convert
   * @return GA4GHVariantResponse json string
   */
  def variantRDDtoJSON(variantRDD: VariantRDD): String = {
    val logger = LoggerFactory.getLogger("GA4GHutil")
    val gaVariants: Array[ga4gh.Variants.Variant] = variantRDD.rdd.collect.map(a => {
      ga4gh.Variants.Variant.newBuilder(variantConverter.convert(a, ConversionStringency.LENIENT, logger))
        .build()
    })
    val result: ga4gh.VariantServiceOuterClass.SearchVariantsResponse = ga4gh.VariantServiceOuterClass
      .SearchVariantsResponse.newBuilder().addAllVariants(gaVariants.toList.asJava).build()

    com.google.protobuf.util.JsonFormat.printer().includingDefaultValueFields().print(result)

  }
}

case class SearchVariantsRequestGA4GH(variantSetId: String,
                                      pageToken: String,
                                      pageSize: Int,
                                      referenceName: String,
                                      callSetIds: Array[String],
                                      start: Long,
                                      end: Long) {

  // converts object to JSON byte array. For testing POSTs.
  def toByteArray(): Array[Byte] = {
    implicit val formats = DefaultFormats
    compact(render(decompose(this))).toCharArray.map(_.toByte)
  }

}

case class SearchFeaturesRequestGA4GH(featureSetId: String,
                                      pageToken: String,
                                      pageSize: Int,
                                      referenceName: String,
                                      start: Long,
                                      end: Long) {

  // converts object to JSON byte array. For testing POSTs.
  def toByteArray(): Array[Byte] = {
    implicit val formats = DefaultFormats
    compact(render(decompose(this))).toCharArray.map(_.toByte)
  }

}

// see proto defintiion: https://github.com/ga4gh/ga4gh-schemas/blob/master/src/main/proto/ga4gh/read_service.proto#L117
case class SearchReadsRequestGA4GH(pageToken: String,
                                   pageSize: Int,
                                   readGroupIds: Array[String],
                                   referenceId: String,
                                   start: Long,
                                   end: Long) {

  // converts object to JSON byte array. For testing POSTs.
  def toByteArray(): Array[Byte] = {
    implicit val formats = DefaultFormats
    compact(render(decompose(this))).toCharArray.map(_.toByte)
  }

}
