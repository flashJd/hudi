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

package org.apache.spark.sql.hudi

import org.apache.spark.sql.SparkSession

import java.io.File

/**
 * Hudi Spark SQL Demo
 */
object TestSparkOnlineTest {
  val tableName = "t1"
  val path = "file://" + new File("../../hudi_table_test").getAbsolutePath
  val dropTable = false

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      master("local[*]").
      appName("TestSparkOnlineTest").
      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      // 扩展Spark SQL，使Spark SQL支持Hudi
      config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension").
      // config("hive.metastore.uris", "thrift://localhost:9083").
      // enableHiveSupport().
      getOrCreate()

      // spark.sql(s"show databases").show()
      // spark.sql(s"use xiamen").show()
    if (dropTable) {
      spark.sql(s"drop table if exists ${tableName}_ro").show()
      spark.sql(s"drop table if exists ${tableName}_rt").show()
      spark.sql(s"drop table if exists ${tableName}").show()
    }

    testCreateTable(spark)
    // testInsertTable(spark)
    // testInsertOverWriteTable(spark)
    testQueryTable(spark)
    // testUpdateTable(spark)
    // testDeleteTable(spark)
    // testMergeTable(spark)
    // testQueryTable(spark)

    spark.stop()
  }

  def testCreateTable(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |create table IF NOT EXISTS $tableName (
         |  uuid string,
         |  name string,
         |  age int,
         |  ts timestamp,
         |  start_date1 date,
         |  end_date1 date
         |) using hudi
         | tblproperties (
         |  primaryKey = 'uuid,start_date1',
         |  type = 'mor',
         |  hoodie.metadata.enable = 'false',
         |  hoodie.index.type = 'BUCKET',
         |  hoodie.bucket.index.num.buckets = '4',
         |  hoodie.bucket.index.hash.field = 'uuid'
         | )
         | partitioned by (`end_date1`)
         | location '${path}'
         |""".stripMargin)
  }

  def testInsertTable(spark: SparkSession): Unit = {
    spark.sql(s"insert into $tableName values ('3','a',10,timestamp '2020-05-20 11:01:21',3)")
    spark.sql(s"insert into $tableName values ('3','c',30,timestamp '2020-05-20 10:03:21',3),('4','d',40,timestamp '2020-05-20 11:04:21',4)")
  }

  def testInsertOverWriteTable(spark: SparkSession): Unit = {
    spark.sql(s"insert overwrite table $tableName values ('1','aaa',10,timestamp '2020-05-20 11:01:21',3)")
  }

  def testQueryTable(spark: SparkSession): Unit = {
    spark.sql(s"select * from $tableName").show(false)
  }

  def testUpdateTable(spark: SparkSession): Unit = {
    spark.sql(s"update $tableName set age = 18 where uuid = 1")
  }

  def testDeleteTable(spark: SparkSession): Unit = {
    spark.sql(s"delete from $tableName where uuid = 1")
  }

  def testMergeTable(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |merge into $tableName as t0
         |using (
         |  select 0 as uuid, 'hudi' as name, 112 as age, timestamp '2020-05-20 11:02:21' as ts, 3 as `partition`,'INSERT' as opt_type union
         |  select 2 as uuid, 'bbbbbb' as name, 20 as age, timestamp '2020-05-22 11:02:21' as ts, 3 as `partition`, 'UPDATE' as opt_type union
         |  select 3 as uuid, 'c' as name, 30 as age, timestamp '2020-05-20 11:03:21' as ts, 4 as `partition`, 'DELETE' as opt_type
         | ) as s0
         |on t0.uuid = s0.uuid
         |when matched and opt_type!='DELETE' then update set *
         |when matched and opt_type='DELETE' then delete
         |when not matched and opt_type!='DELETE' then insert (uuid, name, age, ts, `partition`) values (s0.uuid, s0.name, s0.age, s0.ts, s0.`partition`)
         |""".stripMargin)
  }
}
