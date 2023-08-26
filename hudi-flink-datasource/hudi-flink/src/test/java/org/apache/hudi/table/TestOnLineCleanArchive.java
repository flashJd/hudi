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

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.hudi.utils.TestConfigurations.FIELDS01;
import static org.apache.hudi.utils.TestConfigurations.sql;

@Disabled("used for local test")
public class TestOnLineCleanArchive {
  static TableEnvironment streamTableEnv;
  static File tempFile = new File("./hudi_table_test");
  static boolean UsingHive = false;

  static void execInsertSql(TableEnvironment tEnv, String insert) {
    TableResult tableResult = tEnv.executeSql(insert);
    // wait to finish
    try {
      tableResult.await();
    } catch (InterruptedException | ExecutionException ex) {
      // ignored
    }
  }

  public static void main(String[] args) throws Exception {
    // create filesystem table named source
    FileIOUtils.deleteDirectory(tempFile);
    FileIOUtils.mkdir(tempFile);

    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    settings.getConfiguration().setInteger(RestOptions.PORT, RestOptions.PORT.defaultValue());
    streamTableEnv = TableEnvironmentImpl.create(settings);

    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
    execConf.setString("heartbeat.timeout", "10000000000");
    execConf.setString("execution.checkpointing.interval", "2s");
    execConf.setString("state.checkpoints.dir", "file:///tmp/hudi_table_test_cp");
    execConf.setString("state.backend", "rocksdb");
    execConf.setString("state.backend.incremental", "true");

    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");

    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    String dataGenDDL =
        "create table default_catalog.default_database.datagen(\n"
            + "  uuid varchar(20),\n"
            + "  name varchar(10),\n"
            + "  age int,\n"
            + "  ts timestamp(3),\n"
            + "  `partition` int,\n" // test streaming read with partition field in the middle
            + "  PRIMARY KEY(uuid) NOT ENFORCED\n"
            + ")\n"
            + "PARTITIONED BY (`partition`)\n"
            + "with (\n"
            + "  'connector' = 'datagen',\n"
            + "  'rows-per-second'='2000',\n"
            + "  'number-of-rows'='60000',\n"
            + "  'fields.uuid.kind'='random',\n"
            + "  'fields.uuid.length'='10',\n"
            + "  'fields.name.kind'='random',\n"
            + "  'fields.name.length'='10',\n"
            + "  'fields.partition.kind'='random',\n"
            + "  'fields.partition.min'='1',\n"
            + "  'fields.partition.max'='2',\n"
            + "  'fields.age.kind'='random',\n"
            + "  'fields.age.min'='1',\n"
            + "  'fields.age.max'='120'\n"
            + ")";
    streamTableEnv.executeSql(dataGenDDL);

    metaDataDDL();
    streamTableEnv.executeSql("drop table if exists t1_ro");
    streamTableEnv.executeSql("drop table if exists t1_rt");
    streamTableEnv.executeSql("drop table if exists t1");

    tableDDL();
    String insertInto = "insert into t1 select * from default_catalog.default_database.datagen";
    execInsertSql(streamTableEnv, insertInto);
  }

  @Test
  void readTable() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    settings.getConfiguration().setInteger(RestOptions.PORT, RestOptions.PORT.defaultValue());
    streamTableEnv = TableEnvironmentImpl.create(settings);

    Configuration execConf = streamTableEnv.getConfig().getConfiguration();
    execConf.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
    execConf.setString("heartbeat.timeout", "10000000000");
    execConf.setString("execution.checkpointing.interval", "2s");
    // configure not to retry after failure
    execConf.setString("restart-strategy", "fixed-delay");
    execConf.setString("restart-strategy.fixed-delay.attempts", "0");

    metaDataDDL();

    if (!UsingHive) {
      tableDDL();
    }

    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1_ro limit 10").execute().collect());
    rows.forEach(System.out::println);
  }

  private static void tableDDL() {
    String hoodieTableDDL =
        sql("t1")
            .fields(FIELDS01)
            .option(FlinkOptions.PATH, "file://" + tempFile.getAbsolutePath())
            .option("table.type", "MERGE_ON_READ")
            .option("index.type", "BUCKET")
            .option("index.bootstrap.enabled", "true")
            .option("index.global.enabled", "true")
            .option("write.bucket_assign.tasks", "2")
            .option("hoodie.clean.automatic", "true")
            .option("clean.async.enabled", "true")
            .option("compaction.async.enabled", "true")
            .option("compaction.delta_commits", "1")
            .option("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS")
            .option("hoodie.cleaner.commits.retained", "3")
            .option("hoodie.cleaner.policy.failed.writes", "LAZY")
            .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
            .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider")
            .option("hoodie.clean.allow.multiple", "false")
            .option("hoodie.cleaner.delete.bootstap.base.file", "false")
            .option("hoodie.archive.automatic", "true")
            .option("hoodie.archive.async", "false")
            .option("hoodie.keep.max.commits", "5")
            .option("hoodie.keep.min.commits", "4")
            .option("hoodie.archive.beyond.savepoint", "true")
            // .option("compaction.async.enabled", "false")
            // .option("compaction.schedule.enabled", "false")
            // .option("hoodie.table.services.enabled", "false")
            // .option("clean.async.enabled", "false")
            .option("metadata.enabled", "true")
            .option("hoodie.metadata.index.async", "false")
            .option("hoodie.metadata.index.column.stats.enable", "true")
            .option("hoodie.metadata.index.bloom.filter.enable", "false")
            // .option("hoodie.datasource.query.type", "read_optimized")
            .end();
    streamTableEnv.executeSql(hoodieTableDDL);
  }

  private static void metaDataDDL() {
    if (!UsingHive) {
      return;
    }

    String catalogDDL = "CREATE CATALOG hu WITH (\n"
        + "'type' = 'hudi',\n"
        + "'catalog.path' = 'file:///user/hive/warehouse/xiamen.db',\n"
        + "'hive.conf.dir' = 'file:///project/flink_module/flink-1.14.4/hive_conf',\n"
        + "'mode' = 'hms')";

    streamTableEnv.executeSql(catalogDDL);

    streamTableEnv.executeSql("use catalog hu");
    streamTableEnv.executeSql("use xiamen");
    streamTableEnv.executeSql("show databases").print();
    streamTableEnv.executeSql("show tables").print();
  }

  private static String fullPartitionPath(Path basePath, String partitionPath) {
    if (partitionPath.isEmpty()) {
      return basePath.toString();
    }
    return new Path(basePath, partitionPath).toString();
  }

  private static Long getRecordNum(List<HoodieBaseFile> baseFiles, HoodieMetadataConfig metadataConfig, String basePathStr, String column) {
    if (metadataConfig.enabled() && metadataConfig.isColumnStatsIndexEnabled()) {
      HashSet<String> baseFilePaths = baseFiles.stream().map(HoodieBaseFile::getFileName).collect(Collectors.toCollection(HashSet::new));
      HoodieTableMetadata metadataTable = HoodieTableMetadata.create(
          HoodieFlinkEngineContext.DEFAULT,
          metadataConfig, basePathStr,
          false);

      List<String> encodedTargetColumnNames = Arrays.stream(new String[] {column})
          .map(colName -> new ColumnIndexID(colName).asBase64EncodedString()).collect(Collectors.toList());

      HoodieData<HoodieRecord<HoodieMetadataPayload>> records =
          metadataTable.getRecordsByKeyPrefixes(encodedTargetColumnNames, HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, false);

      HoodieData<Long> columnStats = records.map(record -> {
        HoodieMetadataRecord metadataRecord;
        try {
          metadataRecord = (HoodieMetadataRecord) record.getData().getInsertValue(null, null).orElse(null);
          return metadataRecord.getColumnStatsMetadata();
        } catch (IOException e) {
          throw new HoodieException("Exception while getting insert value from metadata payload");
        }
      }).filter(stat -> baseFilePaths.contains(stat.getFileName())).map(HoodieMetadataColumnStats::getValueCount);

      Long totalRecords = columnStats.collectAsList().stream().reduce(Long::sum).orElse(0L);
      try {
        metadataTable.close();
      } catch (Exception e) {
        throw new HoodieException("Exception while close metadataTable");
      }
      return totalRecords;
    } else {
      Long totalRecords = baseFiles.stream().map(
          baseFile -> {
            Path inputFile = baseFile.getHadoopPath();
            try {
              HadoopInputFile file = HadoopInputFile.fromPath(inputFile, new org.apache.hadoop.conf.Configuration());
              ParquetFileReader reader = ParquetFileReader.open(file);
              long recordNum = reader.getRecordCount();
              reader.close();
              System.out.println("------------baseFile: " + inputFile);
              System.out.println("------------record num: " + reader.getRecordCount());
              return recordNum;
            } catch (Exception e) {
              throw new HoodieException(
                  "file: " + inputFile + " Get record num error: ", e);
            }
          }).reduce(Long::sum).orElse(0L);
      return totalRecords;
    }
  }

  @Test
  void statTable() {
    String basePathStr = tempFile.getAbsolutePath();
    String pk = "uuid";
    boolean mdtEnabled = true;
    boolean columnStatsEnabled = true;
    long smallFileSize = 50 * 1024 * 1024L; // 50M
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(mdtEnabled)
        .withMetadataIndexColumnStats(columnStatsEnabled)
        .build();
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

    String[] partitions = FSUtils.getAllPartitionPaths(HoodieFlinkEngineContext.DEFAULT, metadataConfig, basePathStr).toArray(new String[0]);
    System.out.println("-----All partitions: " + Arrays.toString(partitions));
    System.out.println("-----Partition num: " + partitions.length);

    FileStatus[] allFileStatus = FSUtils.getFilesInPartitions(HoodieFlinkEngineContext.DEFAULT, metadataConfig, basePathStr,
            Arrays.stream(partitions).map(p -> fullPartitionPath(new Path(basePathStr), p)).toArray(String[]::new))
        .values().stream().flatMap(Arrays::stream).toArray(FileStatus[]::new);

    long totalSize = Arrays.stream(allFileStatus).map(FileStatus::getLen).reduce(Long::sum).orElse(0L);
    System.out.println("-----Table totalSize: " + totalSize);
    System.out.println("-----Table total files: " + allFileStatus.length);

    long smallFileNum = Arrays.stream(allFileStatus).map(FileStatus::getLen).filter(len -> len <= smallFileSize).count();
    System.out.println("-----Table small file num: " + smallFileNum);

    HoodieTableFileSystemView fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(HoodieFlinkEngineContext.DEFAULT,
        HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePathStr).build(), metadataConfig);

    List<HoodieBaseFile> baseFiles = Arrays.stream(partitions).flatMap(fileSystemView::getLatestBaseFiles).collect(Collectors.toList());
    long recordNum = getRecordNum(baseFiles, metadataConfig, basePathStr, pk);
    System.out.println("-----Table total record num: " + recordNum);
  }
}
