/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_CLIENT_PORT;
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_QUORUM;
import static org.apache.hudi.sink.bucket.ChainedBucketStreamWriteFunction.HBASE_READ_VERSION;
import static org.apache.hudi.utils.TestConfigurations.FIELDS02;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE02;
import static org.apache.hudi.utils.TestConfigurations.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test cases for {@link TestHoodieChainedTable}. */
public class TestHoodieChainedTable {
  private TableEnvironment streamTableEnv;
  private boolean useHbase = false;
  private Connection connection;

  @TempDir File tempFile;

  public static List<String> CHAIN_DATA_SET_INSERT =
      Arrays.asList(
          "{\"uuid\": \"id3\", \"name\": \"Danny\", \"age\": 23, \"ts\": \"2023-05-03T00:00:02\", \"start_date1\": \"2023-05-03\", \"end_date1\": \"2999-12-31\"}",
          "{\"uuid\": \"id4\", \"name\": \"Lisa\", \"age\": 18, \"ts\": \"2023-05-03T00:00:02\", \"start_date1\": \"2023-05-03\", \"end_date1\": \"2999-12-31\"}",
          "{\"uuid\": \"id3\", \"name\": \"Danny\", \"age\": 33, \"ts\": \"2023-05-04T00:00:02\", \"start_date1\": \"2023-05-04\", \"end_date1\": \"2999-12-31\"}",
          "{\"uuid\": \"id4\", \"name\": \"Lisa\", \"age\": 19, \"ts\": \"2023-05-03T00:00:22\", \"start_date1\": \"2023-05-03\", \"end_date1\": \"2999-12-31\"}",
          "{\"uuid\": \"id4\", \"name\": \"Lisa\", \"age\": 20, \"ts\": \"2023-05-05T00:00:55\", \"start_date1\": \"2023-05-05\", \"end_date1\": \"2999-12-31\"}");

  @BeforeEach
  void beforeEach() {
    if (useHbase) {
      try {
        org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set(ZOOKEEPER_QUORUM, "127.0.0.1");
        hbaseConfig.set(ZOOKEEPER_CLIENT_PORT, "2181");
        connection = ConnectionFactory.createConnection(hbaseConfig);
        Admin admin = connection.getAdmin();
        // Instantiating table descriptor class

        ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor ss =
            (ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor)
                ColumnFamilyDescriptorBuilder.of("_ss");
        ss.setMaxVersions(HBASE_READ_VERSION);
        TableDescriptor tableDescriptor =
            TableDescriptorBuilder.newBuilder(TableName.valueOf("t1")).setColumnFamily(ss).build();
        // Create the table through admin
        admin.createTable(tableDescriptor);
        admin.close();
      } catch (Exception ex) {
        throw new HoodieException("Cannot instantiate hbase connect & create hbase table.", ex);
      }
    }
  }

  @AfterEach
  void afterEach() {
    if (useHbase) {
      try {
        Admin admin = connection.getAdmin();
        // Delete table
        admin.disableTable(TableName.valueOf("t1"));
        admin.deleteTable(TableName.valueOf("t1"));
        admin.close();
        connection.close();
      } catch (Exception ex) {
        throw new HoodieException("Cannot delete hbase table.", ex);
      }
    }
  }

  private static Stream<Arguments> configParams() {
    // Parameters:
    // boolean isTablePartitioned,
    // boolean enableCompaction,
    // int parallelism
    Object[][] data =
        new Object[][] {
          {true, false, 1},
          {true, false, 4},
          {true, true, 1},
          {true, true, 4},
          {false, false, 1},
          {false, false, 4},
          {false, true, 1},
          {false, true, 4}
        };
    return Stream.of(data).map(Arguments::of);
  }

  private static Stream<Arguments> configParamsV2() {
    // Parameters:
    // boolean isTablePartitioned
    Object[][] data = new Object[][] {{true}, {false}};
    return Stream.of(data).map(Arguments::of);
  }

  static void execInsertSql(TableEnvironment tEnv, String insert) {
    TableResult tableResult = tEnv.executeSql(insert);
    // wait to finish
    try {
      tableResult.await();
    } catch (InterruptedException | ExecutionException ex) {
      // ignored
    }
  }

  private Map<String, String> tableDDL(boolean partitionTable, boolean compaction) {
    TestConfigurations.Sql sql =
        sql("t1")
            .fields(FIELDS02)
            .pkField("uuid,start_date1")
            .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
            .option("table.type", "MERGE_ON_READ")
            .option("index.type", "BUCKET")
            .option("hoodie.bucket.index.num.buckets", "4")
            .option("hoodie.bucket.index.hash.field", "uuid")
            .option("hoodie.table.chain.enabled", "true")
            .option("hoodie.table.chain.start.date.column", "start_date1")
            .option("hoodie.table.chain.end.date.column", "end_date1");

    if (compaction) {
      sql.option("compaction.async.enabled", "false")
          .option("compaction.delta_commits", "1")
          .option("hoodie.compact.inline", "true");
    }

    if (partitionTable) {
      sql.partitionField("end_date1");
    } else {
      sql.noPartition();
    }

    if (useHbase) {
      sql.option("chain.search_mode", "HBASE")
          .option("hoodie.index.hbase.table", "t1")
          .option("hoodie.index.hbase.zkquorum", "127.0.0.1")
          .option("hoodie.index.hbase.zkport", "2181")
          // Set hbase get buffer size
          .option("hoodie.index.hbase.get.batch.size", "2");
    }
    String hoodieTableDDL = sql.end();
    if (streamTableEnv != null) {
      streamTableEnv.executeSql(hoodieTableDDL);
    }
    return sql.getOption();
  }

  @ParameterizedTest
  @MethodSource("configParams")
  void testChainedTableWriteAndRead(
      boolean partitionTable, boolean enableCompaction, int parallelism) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setInteger(
        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, parallelism);
    execConf.setString("execution.checkpointing.interval", "2s");

    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");
    tableDDL(partitionTable, enableCompaction);

    String insertInto =
        "insert into t1 values('id0','lisa',18, TIMESTAMP '2023-05-02 00:00:13', Date '2023-05-02', Date '2999-12-31'),"
            + "('id0','lisa',19, TIMESTAMP '2023-05-03 00:00:02', Date '2023-05-03', Date '2999-12-31'),"
            + "('id1','Fabian',31, TIMESTAMP '2023-05-03 00:00:04', Date '2023-05-03', Date '2999-12-31'),"
            + "('id2','jian',20, TIMESTAMP '2023-05-03 00:00:15', Date '2023-05-03', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto);
    String insertInto1 =
        "insert into t1 values('id1','Fabian',35, TIMESTAMP '2023-05-06 00:00:06', Date '2023-05-06', Date '2999-12-31'), "
            + "('id2','jian',21, TIMESTAMP '2023-05-03 00:00:18', Date '2023-05-03', Date '2999-12-31'), "
            + "('id3','yong',30, TIMESTAMP '2023-05-06 00:00:28', Date '2023-05-06', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto1);

    List<Row> rows =
        CollectionUtil.iterableToList(
            () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    String rowsString =
        rows.stream()
            .sorted(Comparator.comparing(o -> o.getField(3).toString()))
            .collect(Collectors.toList())
            .toString();
    String expected =
        "[+I[id0, lisa, 18, 2023-05-02T00:00:13, 2023-05-02, 2023-05-03], "
            + "+I[id0, lisa, 19, 2023-05-03T00:00:02, 2023-05-03, 2999-12-31], "
            + "+I[id1, Fabian, 31, 2023-05-03T00:00:04, 2023-05-03, 2023-05-06], "
            + "+I[id2, jian, 21, 2023-05-03T00:00:18, 2023-05-03, 2999-12-31], "
            + "+I[id1, Fabian, 35, 2023-05-06T00:00:06, 2023-05-06, 2999-12-31], "
            + "+I[id3, yong, 30, 2023-05-06T00:00:28, 2023-05-06, 2999-12-31]]";
    assertEquals(expected, rowsString);
  }

  @ParameterizedTest
  @MethodSource("configParamsV2")
  void testChainedTableWriteAndReadWithMergeWithPendingCompaction(boolean partitionTable) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    execConf.setString("execution.checkpointing.interval", "2s");

    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");
    tableDDL(partitionTable, true);

    String insertInto =
        "insert into t1 values('id1','Fabian',31,TIMESTAMP '2023-05-03 00:00:04', Date '2023-05-03', Date '2999-12-31'),"
            + "('id2','jian',20, TIMESTAMP '2023-05-03 00:00:15', Date '2023-05-03', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto);

    String insertInto1 =
        "insert into t1/*+ OPTIONS('hoodie.compact.inline'='false') */ values('id1','Fabian',35,TIMESTAMP '2023-05-06 00:00:06', Date '2023-05-06', Date '2999-12-31'), "
            + "('id2','jian',21, TIMESTAMP '2023-05-03 00:00:18', Date '2023-05-03', Date '2999-12-31'), "
            + "('id3','yong',15, TIMESTAMP '2023-05-03 00:01:35', Date '2023-05-03', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto1);

    List<Row> rows =
        CollectionUtil.iterableToList(
            () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    String rowsString =
        rows.stream()
            .sorted(Comparator.comparing(o -> o.getField(3).toString()))
            .collect(Collectors.toList())
            .toString();
    String expected =
        "[+I[id1, Fabian, 31, 2023-05-03T00:00:04, 2023-05-03, 2023-05-06], "
            + "+I[id2, jian, 21, 2023-05-03T00:00:18, 2023-05-03, 2999-12-31], "
            + "+I[id3, yong, 15, 2023-05-03T00:01:35, 2023-05-03, 2999-12-31], "
            + "+I[id1, Fabian, 35, 2023-05-06T00:00:06, 2023-05-06, 2999-12-31]]";
    assertEquals(expected, rowsString);

    String insertInto2 =
        "insert into t1/*+ OPTIONS('hoodie.compact.inline'='false') */ values('id1','Fabian',40,TIMESTAMP '2023-05-08 00:00:08', Date '2023-05-08', Date '2999-12-31'), "
            + "('id2','jian',30, TIMESTAMP '2023-05-08 00:00:28', Date '2023-05-08', Date '2999-12-31'), "
            + "('id3','yong',18, TIMESTAMP '2023-05-08 00:01:38', Date '2023-05-08', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto2);

    List<Row> rows1 =
        CollectionUtil.iterableToList(
            () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    String rowsString1 =
        rows1.stream()
            .sorted(Comparator.comparing(o -> o.getField(3).toString()))
            .collect(Collectors.toList())
            .toString();
    String expected1 =
        "[+I[id1, Fabian, 31, 2023-05-03T00:00:04, 2023-05-03, 2023-05-06], "
            + "+I[id2, jian, 21, 2023-05-03T00:00:18, 2023-05-03, 2023-05-08], "
            + "+I[id3, yong, 15, 2023-05-03T00:01:35, 2023-05-03, 2023-05-08], "
            + "+I[id1, Fabian, 35, 2023-05-06T00:00:06, 2023-05-06, 2023-05-08], "
            + "+I[id1, Fabian, 40, 2023-05-08T00:00:08, 2023-05-08, 2999-12-31], "
            + "+I[id2, jian, 30, 2023-05-08T00:00:28, 2023-05-08, 2999-12-31], "
            + "+I[id3, yong, 18, 2023-05-08T00:01:38, 2023-05-08, 2999-12-31]]";
    assertEquals(expected1, rowsString1);

    String insertInto3 =
        "insert into t1/*+ OPTIONS('hoodie.compact.inline'='false') */ values('id3','yong',20, TIMESTAMP '2023-05-11 00:02:11', Date '2023-05-11', Date '2999-12-31'), "
            + "('id1','Fabian',40,TIMESTAMP '2023-05-08 00:00:08', Date '2023-05-08', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto3);

    List<Row> rows2 =
        CollectionUtil.iterableToList(
            () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    String rowsString2 =
        rows2.stream()
            .sorted(Comparator.comparing(o -> o.getField(3).toString()))
            .collect(Collectors.toList())
            .toString();
    String expected2 =
        "[+I[id1, Fabian, 31, 2023-05-03T00:00:04, 2023-05-03, 2023-05-06], "
            + "+I[id2, jian, 21, 2023-05-03T00:00:18, 2023-05-03, 2023-05-08], "
            + "+I[id3, yong, 15, 2023-05-03T00:01:35, 2023-05-03, 2023-05-08], "
            + "+I[id1, Fabian, 35, 2023-05-06T00:00:06, 2023-05-06, 2023-05-08], "
            + "+I[id1, Fabian, 40, 2023-05-08T00:00:08, 2023-05-08, 2999-12-31], "
            + "+I[id2, jian, 30, 2023-05-08T00:00:28, 2023-05-08, 2999-12-31], "
            + "+I[id3, yong, 18, 2023-05-08T00:01:38, 2023-05-08, 2023-05-11], "
            + "+I[id3, yong, 20, 2023-05-11T00:02:11, 2023-05-11, 2999-12-31]]";
    assertEquals(expected2, rowsString2);
  }

  @ParameterizedTest
  @MethodSource("configParamsV2")
  void testChainedTableWriteAndReadWithRollback(boolean partitionTable) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    execConf.setString("execution.checkpointing.interval", "2s");

    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");
    Map<String, String> options = tableDDL(partitionTable, false);

    String insertInto =
        "insert into t1 values('id3','yong',15, TIMESTAMP '2023-05-03 00:01:35', Date '2023-05-03', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto);

    String insertInto1 =
        "insert into t1 values('id3','yong',16, TIMESTAMP '2023-05-04 00:01:35', Date '2023-05-04', Date '2999-12-31'),"
            + "('id4','hua',23, TIMESTAMP '2023-05-03 00:01:35', Date '2023-05-03', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto1);

    Configuration config = new Configuration();
    for (String key : options.keySet()) {
      config.setString(key, options.get(key));
    }
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(config);
    HoodieInstant lastInstant =
        metaClient
            .getActiveTimeline()
            .getCommitsTimeline()
            .getInstants()
            .reduce((first, second) -> second)
            .orElse(null);
    // Delete the commitMetaData file to create a rollback
    File commitMetaData =
        new File(
            metaClient.getBasePathV2().toString()
                + "/.hoodie/"
                + lastInstant.getTimestamp()
                + "."
                + lastInstant.getAction());
    commitMetaData.delete();

    String insertInto2 =
        "insert into t1 values('id3','yong',16, TIMESTAMP '2023-05-04 00:01:35', Date '2023-05-04', Date '2999-12-31'),"
            + "('id4','hua',25, TIMESTAMP '2023-05-04 00:01:36', Date '2023-05-04', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto2);
    assertEquals(metaClient.reloadActiveTimeline().getRollbackTimeline().countInstants(), 1);

    List<Row> rows =
        CollectionUtil.iterableToList(
            () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    String rowsString =
        rows.stream()
            .sorted(Comparator.comparing(o -> o.getField(3).toString()))
            .collect(Collectors.toList())
            .toString();
    String expected =
        "[+I[id3, yong, 15, 2023-05-03T00:01:35, 2023-05-03, 2023-05-04], "
            + "+I[id3, yong, 16, 2023-05-04T00:01:35, 2023-05-04, 2999-12-31], "
            + "+I[id4, hua, 25, 2023-05-04T00:01:36, 2023-05-04, 2999-12-31]]";
    assertEquals(expected, rowsString);
  }

  @ParameterizedTest
  @MethodSource("configParamsV2")
  void testChainedTableWriteAndReadWithMultipleCheckpoints(boolean partitionTable)
      throws Exception {
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    Map<String, String> options = tableDDL(partitionTable, false);
    Configuration conf = new Configuration();
    for (String key : options.keySet()) {
      conf.setString(key, options.get(key));
    }
    String inferredSchema = AvroSchemaConverter.convertToSchema(ROW_TYPE02).toString();
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, inferredSchema);
    conf.setInteger(FlinkOptions.WRITE_TASKS, 4);
    conf.setString(FlinkOptions.TABLE_NAME, "t1");
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, "uuid,start_date1");
    if (partitionTable) {
      conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "end_date1");
    }
    conf.setString(
        FlinkOptions.KEYGEN_CLASS_NAME, "org.apache.hudi.keygen.ComplexAvroKeyGenerator");

    JsonRowDataDeserializationSchema deserializationSchema =
        new JsonRowDataDeserializationSchema(
            ROW_TYPE02, InternalTypeInfo.of(ROW_TYPE02), false, true, TimestampFormat.ISO_8601);

    DataStream<RowData> dataStream =
        execEnv
            // use continuous file source to trigger checkpoint
            .addSource(new BoundedSourceFunction(CHAIN_DATA_SET_INSERT, 2))
            .name("continuous_file_source")
            .setParallelism(1)
            .map(
                record ->
                    deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)));

    DataStream<HoodieRecord> hoodieRecordDataStream =
        Pipelines.bootstrap(conf, ROW_TYPE02, dataStream);
    DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(conf, hoodieRecordDataStream);
    execEnv.addOperator(pipeline.getTransformation());
    execEnv.execute();

    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    execConf.setString("execution.checkpointing.interval", "2s");

    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");
    tableDDL(partitionTable, false);

    List<Row> rows =
        CollectionUtil.iterableToList(
            () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    String rowsString =
        rows.stream()
            .sorted(Comparator.comparing(o -> o.getField(3).toString()))
            .collect(Collectors.toList())
            .toString();
    String expected =
        "[+I[id3, Danny, 23, 2023-05-03T00:00:02, 2023-05-03, 2023-05-04], "
            + "+I[id4, Lisa, 19, 2023-05-03T00:00:22, 2023-05-03, 2023-05-05], "
            + "+I[id3, Danny, 33, 2023-05-04T00:00:02, 2023-05-04, 2999-12-31], "
            + "+I[id4, Lisa, 20, 2023-05-05T00:00:55, 2023-05-05, 2999-12-31]]";
    assertEquals(expected, rowsString);
  }

  @ParameterizedTest
  @MethodSource("configParamsV2")
  void testChainedTableWriteAndReadWithHbaseBuffer(boolean partitionTable) {
    if (!useHbase) {
      return;
    }
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    execConf.setString("execution.checkpointing.interval", "2s");

    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");
    tableDDL(partitionTable, false);

    String insertInto =
        "insert into t1 values('id3','yong',15, TIMESTAMP '2023-05-03 00:01:35', Date '2023-05-03', Date '2999-12-31'),"
            + "('id3','yong',17, TIMESTAMP '2023-05-05 00:01:55', Date '2023-05-05', Date '2999-12-31'),"
            + "('id3','yong',18, TIMESTAMP '2023-05-08 00:01:38', Date '2023-05-08', Date '2999-12-31')";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows =
        CollectionUtil.iterableToList(
            () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    String rowsString =
        rows.stream()
            .sorted(Comparator.comparing(o -> o.getField(3).toString()))
            .collect(Collectors.toList())
            .toString();
    String expected =
        "[+I[id3, yong, 15, 2023-05-03T00:01:35, 2023-05-03, 2023-05-05], "
            + "+I[id3, yong, 17, 2023-05-05T00:01:55, 2023-05-05, 2023-05-08], "
            + "+I[id3, yong, 18, 2023-05-08T00:01:38, 2023-05-08, 2999-12-31]]";
    assertEquals(expected, rowsString);

    String insertInto1 =
        "insert into t1 values('id3','yong',20, TIMESTAMP '2023-05-11 00:02:11', Date '2023-05-11', Date '2999-12-31')";

    execInsertSql(streamTableEnv, insertInto1);
    List<Row> rows1 =
        CollectionUtil.iterableToList(
            () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    String rowsString1 =
        rows1.stream()
            .sorted(Comparator.comparing(o -> o.getField(3).toString()))
            .collect(Collectors.toList())
            .toString();
    String expected1 =
        "[+I[id3, yong, 15, 2023-05-03T00:01:35, 2023-05-03, 2023-05-05], "
            + "+I[id3, yong, 17, 2023-05-05T00:01:55, 2023-05-05, 2023-05-08], "
            + "+I[id3, yong, 18, 2023-05-08T00:01:38, 2023-05-08, 2023-05-11], "
            + "+I[id3, yong, 20, 2023-05-11T00:02:11, 2023-05-11, 2999-12-31]]";
    assertEquals(expected1, rowsString1);
  }

  public static class BoundedSourceFunction implements SourceFunction<String>, CheckpointListener {
    private final List<String> rowData;
    private final int checkpoints;
    private final AtomicInteger currentCP = new AtomicInteger(0);

    private volatile boolean isRunning = true;

    public BoundedSourceFunction(List<String> rowData, int checkpoints) {
      this.rowData = rowData;
      this.checkpoints = checkpoints;
    }

    @Override
    public void run(SourceContext<String> context) throws Exception {
      int oldCP = this.currentCP.get();
      boolean finish = false;
      while (isRunning) {
        int batchSize = this.rowData.size() / this.checkpoints;
        int start = batchSize * oldCP;
        synchronized (context.getCheckpointLock()) {
          for (int i = start; i < start + batchSize; i++) {
            if (i >= this.rowData.size()) {
              finish = true;
              break;
              // wait for the next checkpoint and exit
            }
            context.collect(this.rowData.get(i));
          }
        }
        oldCP++;
        while (this.currentCP.get() < oldCP) {
          synchronized (context.getCheckpointLock()) {
            context.getCheckpointLock().wait(10);
          }
        }
        if (finish || !isRunning) {
          return;
        }
      }
    }

    @Override
    public void cancel() {
      this.isRunning = false;
    }

    @Override
    public void notifyCheckpointComplete(long l) {
      this.currentCP.incrementAndGet();
    }
  }
}
