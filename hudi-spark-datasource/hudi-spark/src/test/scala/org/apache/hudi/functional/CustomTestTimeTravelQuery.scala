/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.common.model.{HoodieAvroPayload, HoodieTableType, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator, SimpleAvroKeyGenerator}
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.text.SimpleDateFormat
import java.util.function.{Consumer, Predicate}

class CustomTestTimeTravelQuery extends HoodieClientTestBase {
  var spark: SparkSession =_
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "version",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach override def setUp() {
    setTableName("TestHoodieTable")
    initPathLocal()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  def toJavaConsumer[T](consumer: (T) => Unit): Consumer[T] ={
    new Consumer[T] {
      override def accept(t: T): Unit = {
        consumer(t)
      }
    }
  }

  def toJavaPredicate[T](predicate: (T) => Boolean): Predicate[T] ={
    new Predicate[T] {
      override def test(t: T): Boolean = {
        predicate(t)
      }
    }
  }


  @Test
  def testTimeTravelQueryWithImplicitSchemaEvolution(): Unit = {
    metaClient = HoodieTableMetaClient.withPropertyBuilder
      .setTableType(HoodieTableType.COPY_ON_WRITE)
      .setTableName("aa")
      .setRecordKeyFields("uuid")
      .setPayloadClass(classOf[HoodieAvroPayload])
      .setPreCombineField("ts")
      .setPartitionFields("partition")
      .initTable(hadoopConf, basePath)

    val _spark = spark
    import _spark.implicits._

    val firstCommit = metaClient.getActiveTimeline.filterCompletedInstants().nthInstant(0).get().getTimestamp
    val secondCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

    // Query as of secondCommit
    spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, secondCommit)
      .option(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key, "true")
      .load(basePath)
      .show()
    // assertEquals(Row(1, "a1", 10, 1000), result1)

    spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, firstCommit)
      .load(basePath)
      .show()
  }

  @Test
  def testTimeTravelQueryWithExplicitSchemaEvolution(): Unit = {
    metaClient = HoodieTableMetaClient.withPropertyBuilder
      .setTableType(HoodieTableType.MERGE_ON_READ)
      .setTableName("aa")
      .setRecordKeyFields("uuid")
      .setPayloadClass(classOf[HoodieAvroPayload])
      .setPreCombineField("ts")
      .setPartitionFields("partition")
      .initTable(hadoopConf, basePath)

    val _spark = spark
    import _spark.implicits._

    // implicit evolution has two instants, explicit evolution has five instants, first insert, second addColumn, third renameColumn, forth updateColumnType, last insert
    metaClient.getActiveTimeline.filterCompletedInstants().getInstants()
      .filter(toJavaPredicate((instance:HoodieInstant) => instance.getAction != HoodieTimeline.ROLLBACK_ACTION))
      .forEach(toJavaConsumer((instance:HoodieInstant) =>
      spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, instance.getTimestamp)
      .load(basePath)
      .show()))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSparkTimeTravelQueryWithSchemaEvolutionDeleteColumn(tableType: HoodieTableType): Unit = {
    initMetaClient(tableType)
    val _spark = spark
    import _spark.implicits._

    // First write
    val df1 = Seq((1, "a1", 10, 1000)).toDF("id", "name", "value", "version")
    df1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(PARTITIONPATH_FIELD.key, "name")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()
    val firstCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

    // Second write
    val df2 = Seq((2, "a2", 1002)).toDF("id", "name", "version")
    df2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(PARTITIONPATH_FIELD.key, "name")
      .mode(SaveMode.Append)
      .save(basePath)
    metaClient.reloadActiveTimeline()
    val secondCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

    val tableSchemaResolver = new TableSchemaResolver(metaClient)

    // Query as of firstCommitTime
    val result1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, firstCommit)
      .load(basePath)
      .select("id", "name", "value", "version")
      .take(1)(0)
    assertEquals(Row(1, "a1", 10, 1000), result1)

    // Query as of secondCommitTime
    val result2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, secondCommit)
      .load(basePath)
      .select("id", "name", "version")
      .take(2)(1)
    assertEquals(Row(2, "a2", 1002), result2)
  }
}


