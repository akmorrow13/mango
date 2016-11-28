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

import net.liftweb.json._
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
import org.bdgenomics.mango.layout.BedRowJson
import org.bdgenomics.mango.util.MangoFunSuite

class ModelMaterializationSuite extends MangoFunSuite {

  implicit val formats = DefaultFormats

  // test reference data
  var referencePath = resourcePath("mm10_chrM.fa")
  val region = ReferenceRegion("chrM", 0, 500)

  val dict = new SequenceDictionary(Vector(SequenceRecord("chrM", 16699L)))

  sparkTest("assert model scores sequences over small range") {
    val reference = new AnnotationMaterialization(sc, referencePath)

    val filePaths = List("model1", "model2")

    val data = new ModelMaterialization(reference, filePaths)

    val region = new ReferenceRegion("chrM", 1L, 500L)

    val json = data.get(region)
    assert(json.size == filePaths.length)
    val model1 = parse(json.get("model1").get).extract[Array[BedRowJson]]

    assert(model1.length == 7)

  }

  sparkTest("assert model does not return over large regions") {
    val reference = new AnnotationMaterialization(sc, referencePath)

    val filePaths = List("model1", "model2")

    val data = new ModelMaterialization(reference, filePaths)

    val region = new ReferenceRegion("chrM", 1L, 10000L)

    val json = data.get(region)
    assert(json.size == filePaths.length)
    val model1 = parse(json.get("model1").get).extract[Array[BedRowJson]]

    assert(model1.length == 0)

  }
}
