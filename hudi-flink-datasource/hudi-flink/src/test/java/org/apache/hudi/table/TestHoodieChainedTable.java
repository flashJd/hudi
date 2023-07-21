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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.utils.TestConfigurations.FIELDS02;
import static org.apache.hudi.utils.TestConfigurations.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test cases for {@link TestHoodieChainedTable}. */
public class TestHoodieChainedTable {
  private TableEnvironment streamTableEnv;
  @TempDir File tempFile;

  private static Stream<Arguments> configParams() {
    // Parameters:
    //   boolean isTablePartitioned,
    //   boolean enableCompaction,
    //   int parallelism
    Object[][] data = new Object[][] {
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

  private static Stream<Arguments> configParams1() {
    // Parameters:
    //   boolean isTablePartitioned,
    //   int parallelism
    Object[][] data = new Object[][] {
        {true},
        {false}
    };
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

  @ParameterizedTest
  @MethodSource("configParams")
  void testChainedTableWriteAndRead(boolean partitionTable, boolean enableCompaction, int parallelism) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, parallelism);
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
  @MethodSource("configParams1")
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

  private void tableDDL(boolean partitionTable, boolean compaction) {
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
    String hoodieTableDDL = sql.end();
    streamTableEnv.executeSql(hoodieTableDDL);
  }
}
