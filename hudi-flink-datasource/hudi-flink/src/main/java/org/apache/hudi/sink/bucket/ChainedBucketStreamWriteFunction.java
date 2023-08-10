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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieDependentSystemUnavailableException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.sink.utils.PayloadCreation;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_CLIENT_PORT;
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_QUORUM;
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_ZNODE_PARENT;
import static org.apache.hadoop.hbase.security.SecurityConstants.MASTER_KRB_PRINCIPAL;
import static org.apache.hadoop.hbase.security.SecurityConstants.REGIONSERVER_KRB_PRINCIPAL;
import static org.apache.hadoop.hbase.security.User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY;
import static org.apache.hadoop.hbase.security.User.HBASE_SECURITY_CONF_KEY;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS;
import static org.apache.hudi.configuration.FlinkOptions.CHAIN_SEARCH_MODE;
import static org.apache.hudi.configuration.FlinkOptions.HIVE_STYLE_PARTITIONING;
import static org.apache.hudi.configuration.FlinkOptions.METADATA_ENABLED;

/**
 * A stream write function with bucket hash index with chain table enabled.
 *
 * <p>The task holds a fresh new local index: {(partition + bucket number) &rarr fileId} mapping,
 * this index is used for deciding whether the incoming records in an UPDATE or INSERT. The index is
 * local because different partition paths have separate items in the index.
 *
 * @param <I> the input type
 */
public class ChainedBucketStreamWriteFunction<I> extends BucketStreamWriteFunction<I> {

  private static final Logger LOG = LoggerFactory.getLogger(ChainedBucketStreamWriteFunction.class);

  private static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("ss");

  private static final byte[] AVRO_DATA = Bytes.toBytes("avro_data");

  private static final byte[] COMMIT_TS_COLUMN = Bytes.toBytes("commit_ts");

  private static final byte[] START_DATE = Bytes.toBytes("start_date");

  public static final int HBASE_READ_VERSION = 5;

  /**
   * Chain table's original pk to HoodieRecord mapping in 2999 partition. Map(original pk ->
   * HoodieRecord).
   */
  private transient ExternalSpillableMap<String, HoodieRecord<?>> openChainRecords;

  private transient List<HoodieRecord<?>> unCheckedRecords;

  private transient Map<String, TreeMap<String, HoodieRecord<?>>> unSentRecords;

  private static transient Connection hbaseConnection;

  private transient HoodieWriteConfig writeConfig;

  private transient Schema schema;

  private transient Option<BaseKeyGenerator> keyGeneratorOpt;

  /** Used to create DELETE payload. */
  private transient PayloadCreation payloadCreation;

  /**
   * Constructs a BucketStreamWriteFunction.
   *
   * @param config The config options
   */
  public ChainedBucketStreamWriteFunction(Configuration config) {
    super(config);
  }

  private boolean checkIfValidCommit(HoodieTableMetaClient metaClient, String commitTs) {
    HoodieTimeline commitTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
    // Check if the last commit ts for this row is 1) present in the timeline or
    // 2) is less than the first commit ts in the timeline
    return !commitTimeline.empty()
        && commitTimeline.containsOrBeforeTimelineStarts(commitTs);
  }

  private void processNewRecord(HoodieRecord<?> record, String originKey) {
    if (record.getOperation().equals(HoodieOperation.DELETE)) {
      // the coming record is a close chain record and has no oldHoodieRecord, ignore it
      LOG.warn(String.format("The coming record is a close chain record and has no oldHoodieRecord, ignore it %s", record));
    } else {
      // new record without old record
      bufferRecord(record);
      updateChainIndex(originKey, record, null, true);
    }
  }

  private void procoseeUncheckedRecordsWithHbase(List<HoodieRecord<?>> records) {
    // Use originKey to get old records stored in hbase
    Result[] results;
    List<HoodieRecord<?>> comingRecordsInHbase = new ArrayList<>();
    List<HoodieRecord<?>> comingRecordsInMemory = new ArrayList<>();
    try (HTable hTable =
        (HTable) hbaseConnection.getTable(TableName.valueOf(writeConfig.getHbaseTableName()))) {
      List<Get> statements = new ArrayList<>();
      records.forEach(
          record -> {
            List<String> originKeyList =
                BucketIdentifier.getHashKeys(record.getKey(), this.indexKeyFields);
            String originKey = String.join(",", originKeyList);
            if (unSentRecords.get(originKey) == null) {
              statements.add(generateStatement(originKey));
              comingRecordsInHbase.add(record);
            } else {
              comingRecordsInMemory.add(record);
            }
          });
      results = hTable.get(statements);
    } catch (IOException e) {
      throw new HoodieException("Failed to get old record with HBase Client", e);
    }

    // Compare record with old record to do chain closing
    for (HoodieRecord<?> comingHoodieRecord : comingRecordsInMemory) {
      List<String> originKeyList =
          BucketIdentifier.getHashKeys(comingHoodieRecord.getKey(), this.indexKeyFields);
      String originKey = String.join(",", originKeyList);
      HoodieRecord<?> unSentRecord = unSentRecords.get(originKey).lastEntry().getValue();
      String oldStartDate = unSentRecords.get(originKey).lastEntry().getKey();
      try {
        doChain(
            comingHoodieRecord, unSentRecord, comingHoodieRecord.getPartitionPath(), oldStartDate);
      } catch (Exception e) {
        throw new HoodieException("Failed to doChain.", e);
      }
    }

    List<HoodieRecord<?>> comingRecordRowKeysToRollback = new ArrayList<>();
    for (int i = 0; i < results.length; i++) {
      Result result = results[i];
      HoodieRecord<?> record = comingRecordsInHbase.get(i);
      List<String> originKeyList =
          BucketIdentifier.getHashKeys(record.getKey(), this.indexKeyFields);
      String originKey = String.join(",", originKeyList);
      if (unSentRecords.get(originKey) == null) {
        if (result.getRow() == null) {
          processNewRecord(record, originKey);
        } else {
          byte[] avroBytes = result.getValue(SYSTEM_COLUMN_FAMILY, AVRO_DATA);
          String commitTs = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN));
          if (!checkIfValidCommit(this.metaClient, commitTs)) {
            comingRecordRowKeysToRollback.add(record);
            continue;
          }
          String oldStartDate = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, START_DATE));
          processHbaseRecord(record, avroBytes, oldStartDate);
        }
      } else {
        try {
          HoodieRecord<?> unSentRecord = unSentRecords.get(originKey).lastEntry().getValue();
          String oldStartDate = unSentRecords.get(originKey).lastEntry().getKey();
          doChain(record, unSentRecord, record.getPartitionPath(), oldStartDate);
        } catch (Exception e) {
          throw new HoodieException("Failed to doChain.", e);
        }
      }
    }

    Result[] rowKeysToRollbackResults;
    try (HTable hTable =
             (HTable) hbaseConnection.getTable(TableName.valueOf(writeConfig.getHbaseTableName()))) {
      List<Get> statements = new ArrayList<>();
      comingRecordRowKeysToRollback.forEach(
          record -> {
            List<String> originKeyList =
                BucketIdentifier.getHashKeys(record.getKey(), this.indexKeyFields);
            String originKey = String.join(",", originKeyList);
            statements.add(generateStatement(originKey,HBASE_READ_VERSION));
          });
      rowKeysToRollbackResults = hTable.get(statements);
    } catch (IOException e) {
      throw new HoodieException("Failed to get old rollback record with HBase Client", e);
    }

    for (int i = 0; i < rowKeysToRollbackResults.length; i++) {
      Result result = rowKeysToRollbackResults[i];
      HoodieRecord<?> record = comingRecordRowKeysToRollback.get(i);
      List<String> originKeyList =
          BucketIdentifier.getHashKeys(record.getKey(), this.indexKeyFields);
      String originKey = String.join(",", originKeyList);
      if (unSentRecords.get(originKey) == null) {
        List<Cell> cells = result.getColumnCells(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN);
        int j = 1;
        for (; j < cells.size(); j++) {
          Cell cell = cells.get(j);
          String commitTs = Bytes.toString(CellUtil.cloneValue(cell));
          if (!checkIfValidCommit(this.metaClient, commitTs)) {
            continue;
          }

          byte[] avroBytes = CellUtil.cloneValue(result.getColumnCells(SYSTEM_COLUMN_FAMILY, AVRO_DATA).get(j));
          String oldStartDate = Bytes.toString(CellUtil.cloneValue(result.getColumnCells(SYSTEM_COLUMN_FAMILY, START_DATE).get(j)));
          processHbaseRecord(record, avroBytes, oldStartDate);
          break;
        }

        if (j == cells.size()) {
          // No need to delete the rowKey in hbase, as updateChainIndex will overwrite it
          processNewRecord(record, originKey);
        }
      } else {
        try {
          HoodieRecord<?> unSentRecord = unSentRecords.get(originKey).lastEntry().getValue();
          String oldStartDate = unSentRecords.get(originKey).lastEntry().getKey();
          doChain(record, unSentRecord, record.getPartitionPath(), oldStartDate);
        } catch (Exception e) {
          throw new HoodieException("Failed to doChain.", e);
        }
      }
    }
  }

  private void bufferUnCheckedRecords(HoodieRecord<?> value) {
    unCheckedRecords.add(value);
    if (unCheckedRecords.size() >= writeConfig.getHbaseIndexGetBatchSize()) {
      processUnCheckedRecords();
    }
  }

  private void processUnCheckedRecords() {
    procoseeUncheckedRecordsWithHbase(unCheckedRecords);
    unCheckedRecords.clear();
  }

  private void updateChainHbaseIndex() {
    try (BufferedMutator mutator =
             hbaseConnection.getBufferedMutator(
                 TableName.valueOf(writeConfig.getHbaseTableName()))) {
      List<Mutation> mutations = new ArrayList<>();
      for (String key : unSentRecords.keySet()) {
        Put put = new Put(Bytes.toBytes(key));
        HoodieRecord<?> unSentRecord = unSentRecords.get(key).lastEntry().getValue();
        BaseAvroPayload payload;
        if (unSentRecord == null) {
          put.addColumn(SYSTEM_COLUMN_FAMILY, AVRO_DATA, new byte[0]);
        } else {
          payload = (BaseAvroPayload) unSentRecord.getData();
          put.addColumn(SYSTEM_COLUMN_FAMILY, AVRO_DATA, payload.recordBytes);
        }
        String startDate = unSentRecords.get(key).lastEntry().getKey();
        put.addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN, Bytes.toBytes(this.currentInstant));
        put.addColumn(SYSTEM_COLUMN_FAMILY, START_DATE, Bytes.toBytes(startDate));
        mutations.add(put);
      }
      mutator.mutate(mutations);
      mutator.flush();
      mutations.clear();
      unSentRecords.clear();
    } catch (IOException e) {
      throw new HoodieException("HBase client failed.", e);
    }
  }

  private void updateChainIndex(String originKey, HoodieRecord<?> record, String startDate, boolean updateOpenChainRecords) {
    if (startDate == null) {
      startDate = BucketIdentifier.getHashKeys(record.getKey(), writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN)).get(0);
    }
    unSentRecords.computeIfAbsent(originKey, k -> new TreeMap<>()).put(startDate, record);

    if (config.getString(CHAIN_SEARCH_MODE).equals(ChainedTableSearchMode.SPILL_MAP.name()) && updateOpenChainRecords) {
      openChainRecords.put(originKey, record);
    }
  }

  private Connection getHBaseConnection() {
    org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
    String quorum = writeConfig.getHbaseZkQuorum();
    hbaseConfig.set(ZOOKEEPER_QUORUM, quorum);
    String zkZnodeParent = writeConfig.getHBaseZkZnodeParent();
    if (zkZnodeParent != null) {
      hbaseConfig.set(ZOOKEEPER_ZNODE_PARENT, zkZnodeParent);
    }
    String port = String.valueOf(writeConfig.getHbaseZkPort());
    hbaseConfig.set(ZOOKEEPER_CLIENT_PORT, port);

    try {
      String authentication = writeConfig.getHBaseIndexSecurityAuthentication();
      if (authentication.equals("kerberos")) {
        hbaseConfig.set(HBASE_SECURITY_CONF_KEY, "kerberos");
        hbaseConfig.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        hbaseConfig.set(HBASE_SECURITY_AUTHORIZATION_CONF_KEY, "true");
        hbaseConfig.set(
            REGIONSERVER_KRB_PRINCIPAL, writeConfig.getHBaseIndexRegionserverPrincipal());
        hbaseConfig.set(MASTER_KRB_PRINCIPAL, writeConfig.getHBaseIndexMasterPrincipal());

        String principal = writeConfig.getHBaseIndexKerberosUserPrincipal();
        String keytab = writeConfig.getHBaseIndexKerberosUserKeytab();

        UserGroupInformation.setConfiguration(hbaseConfig);
        UserGroupInformation ugi =
            UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        return ugi.doAs(
            (PrivilegedExceptionAction<Connection>) () -> (Connection) ConnectionFactory.createConnection(hbaseConfig));
      } else {
        return ConnectionFactory.createConnection(hbaseConfig);
      }
    } catch (IOException | InterruptedException e) {
      throw new HoodieDependentSystemUnavailableException(
          HoodieDependentSystemUnavailableException.HBASE, quorum + ":" + port, e);
    }
  }

  private Get generateStatement(String key) {
    return new Get(Bytes.toBytes(key));
  }

  private Get generateStatement(String key, int version) {
    try {
      return new Get(Bytes.toBytes(key)).readVersions(version);
    } catch (Exception ex) {
      throw new HoodieException("Hbase get with version failed.", ex);
    }
  }

  private void processHbaseRecord(HoodieRecord<?> record, byte[] avroBytes, String oldStartDate) {
    try {
      if (avroBytes.length == 0) {
        LOG.warn(String.format("The old record has already chain closed, new record: %s, old record chain close date: %s", record, oldStartDate));
        doChain(record, null, record.getPartitionPath(), oldStartDate);
      } else {
        GenericRecord indexedRecord = HoodieAvroUtils.bytesToAvro(avroBytes, schema);
        String key = KeyGenUtils.getRecordKeyFromGenericRecord(indexedRecord, keyGeneratorOpt);
        String partition =
            KeyGenUtils.getPartitionPathFromGenericRecord(indexedRecord, keyGeneratorOpt);
        HoodieKey hoodieKey = new HoodieKey(key, partition);
        HoodieRecord<?> oldHoodieRecord =
            new HoodieAvroRecord<>(hoodieKey, payloadCreation.createPayload(indexedRecord));
        doChain(record, oldHoodieRecord, record.getPartitionPath(), null);
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to construct oldHoodieRecord and doChain.", e);
    }
  }

  private Pair<HoodieRecord<?>, String> getRecordOfOriginKey(String partition, int bucketNum, String originKey, String lateDate, String oldStartDate) throws Exception {
    Pair<HoodieRecord<?>, String> target = null;
    String firstStartDate = oldStartDate;
    // check if the record to separate is in the same batch
    HoodieRecord<?> recordInBatch =
        unSentRecords.get(originKey) == null ? null : (unSentRecords.get(originKey).floorEntry(lateDate) == null ? null : unSentRecords.get(originKey).floorEntry(lateDate).getValue());
    if (recordInBatch != null) {
      LOG.info(String.format("Late record in the same batch, lateDate: %s, separate oldRecord: %s", lateDate, recordInBatch));
      return new ImmutablePair<>(recordInBatch, unSentRecords.get(originKey).higherKey(lateDate));
    } else {
      if (!partition.equals("") && unSentRecords.get(originKey) != null) {
        partition = unSentRecords.get(originKey).higherKey(lateDate);
        firstStartDate = partition;
      }
    }

    if (!partition.equals("")) {
      partition = config.getBoolean(HIVE_STYLE_PARTITIONING) ? writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN) + "=" + partition : partition;
    }

    TableFileSystemView fsView = this.writeClient.getHoodieTable().getHoodieView();
    if (!fsView.getLastInstant().isPresent()) {
      LOG.info("No instant completed now, exit getRecordOfOriginKey.");
      return new ImmutablePair<>(null, firstStartDate);
    }
    String latestCommit = fsView.getLastInstant().get().getTimestamp();
    if (!(fsView instanceof TableFileSystemView.SliceViewWithLatestSlice)) {
      throw new RuntimeException(
          "fsView is not a instance of TableFileSystemView.SliceViewWithLatestSlice");
    }

    FileSlice latestFileSlice = ((TableFileSystemView.SliceViewWithLatestSlice) fsView)
        .getLatestMergedFileSlicesBeforeOrOn(partition, latestCommit)
        .filter(fileSlice -> {
          String fileID = fileSlice.getFileId();
          int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileID);
          return bucketNumber == bucketNum;
        }).findFirst().orElse(null);
    if (latestFileSlice == null) {
      LOG.info("fileSlice empty");
      return new ImmutablePair<>(null, firstStartDate);
    }

    LOG.info(String.format("Try to find originKey: %s in fileID: %s, partition: %s.", originKey, latestFileSlice.getFileId(), partition));

    HoodieBaseFile baseFile;
    HoodieFileReader<GenericRecord> baseFileReader;
    Iterator<GenericRecord> baseReaderIterator;

    Schema schemaWithMeta = schema;
    if (metaClient.getTableConfig().populateMetaFields()) {
      schemaWithMeta =
          HoodieAvroUtils.addMetadataFields(schema, writeConfig.allowOperationMetadataField());
    }

    // generate base file reader
    baseFile = latestFileSlice.getBaseFile().orElse(null);

    // generate log files reader
    List<String> logFiles =
        latestFileSlice
            .getLogFiles()
            .sorted(HoodieLogFile.getLogFileComparator())
            .map(logFile -> logFile.getPath().toString())
            .collect(toList());
    String maxInstantTime =
        metaClient
            .getActiveTimeline()
            .getTimelineOfActions(
                CollectionUtils.createSet(
                    HoodieTimeline.COMMIT_ACTION,
                    HoodieTimeline.ROLLBACK_ACTION,
                    HoodieTimeline.DELTA_COMMIT_ACTION))
            .filterCompletedInstants()
            .lastInstant()
            .get()
            .getTimestamp();

    HoodieMergedLogRecordScanner scanner =
        HoodieMergedLogRecordScanner.newBuilder()
            .withFileSystem(metaClient.getFs())
            .withBasePath(metaClient.getBasePathV2().toString())
            .withLogFilePaths(logFiles)
            .withReaderSchema(schemaWithMeta)
            .withLatestInstantTime(maxInstantTime)
            .withInternalSchema(
                SerDeHelper.fromJson(writeConfig.getInternalSchema())
                    .orElse(InternalSchema.getEmptyInternalSchema()))
            .withMaxMemorySizeInBytes(StreamerUtil.getMaxCompactionMemoryInBytes(config))
            //        .withReadBlocksLazily(writeConfig.getCompactionLazyBlockReadEnabled())
            .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
            .withSpillableMapBasePath(writeConfig.getSpillableMapBasePath())
            .withDiskMapType(writeConfig.getCommonConfig().getSpillableDiskMapType())
            .withBitCaskDiskMapCompressionEnabled(
                writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
            .withOperationField(writeConfig.allowOperationMetadataField())
            .build();

    Map<String, HoodieRecord<? extends HoodieRecordPayload>> keyToNewRecords = scanner.getRecords();
    Set<String> writtenRecordKeys = new HashSet<>();

    // write records in base file
    if (baseFile != null) {
      Path baseFilePath = new Path(baseFile.getPath());
      HoodieKey hoodieKey;
      baseFileReader =
          HoodieFileReaderFactory.getFileReader(
              new org.apache.hadoop.conf.Configuration(), baseFilePath);
      baseReaderIterator = baseFileReader.getRecordIterator();

      while (baseReaderIterator.hasNext()) {
        GenericRecord baseRecord = baseReaderIterator.next();
        String key = KeyGenUtils.getRecordKeyFromGenericRecord(baseRecord, keyGeneratorOpt);
        writtenRecordKeys.add(key);
        // get original pk
        List<String> originKeyList =
            BucketIdentifier.getHashKeys(key, this.indexKeyFields);
        String originKeyInFileGroup = String.join(",", originKeyList);
        if (!originKey.equals(originKeyInFileGroup)) {
          continue;
        }
        hoodieKey = new HoodieKey(key, KeyGenUtils.getPartitionPathFromGenericRecord(baseRecord, keyGeneratorOpt));
        if (keyToNewRecords.containsKey(key)) {
          HoodieRecord<? extends HoodieRecordPayload> hoodieRecord =
              keyToNewRecords.get(key).newInstance();

          Option<IndexedRecord> combinedAvroRecord =
              hoodieRecord
                  .getData()
                  .combineAndGetUpdateValue(
                      baseRecord,
                      schemaWithMeta,
                      StreamerUtil.getPayloadConfig(config).getProps());
          if (combinedAvroRecord.isPresent()) {
            GenericRecord record = (GenericRecord) combinedAvroRecord.get();
            GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(record, schema, Collections.emptyMap());
            if (partition.equals("")) {
              String startDate = HoodieAvroUtils.getNestedFieldVal(record, writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN), false, false).toString();
              String endDate = HoodieAvroUtils.getNestedFieldVal(record, writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN), false, false).toString();
              if (lateDate.compareTo(startDate) < 0 || lateDate.compareTo(endDate) >= 0) {
                firstStartDate = firstStartDate.compareTo(startDate) < 0 ? firstStartDate : startDate;
              } else {
                firstStartDate = endDate;
                target = new ImmutablePair<>(new HoodieAvroRecord<>(hoodieRecord.getKey(), payloadCreation.createPayload(rewriteRecord)), firstStartDate);
              }
              updateChainIndex(originKey, new HoodieAvroRecord<>(hoodieRecord.getKey(), payloadCreation.createPayload(rewriteRecord)), null, false);
              continue;
            }
            baseFileReader.close();
            scanner.close();
            return new ImmutablePair<>(new HoodieAvroRecord<>(hoodieRecord.getKey(), payloadCreation.createPayload(rewriteRecord)), firstStartDate);
          }
        } else {
          GenericRecord rewriteRecord =
              HoodieAvroUtils.rewriteRecordWithNewSchema(
                  baseRecord, schema, Collections.emptyMap());
          if (partition.equals("")) {
            String startDate = HoodieAvroUtils.getNestedFieldVal(rewriteRecord, writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN), false, false).toString();
            String endDate = HoodieAvroUtils.getNestedFieldVal(rewriteRecord, writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN), false, false).toString();
            if (lateDate.compareTo(startDate) < 0 || lateDate.compareTo(endDate) >= 0) {
              firstStartDate = firstStartDate.compareTo(startDate) < 0 ? firstStartDate : startDate;
            } else {
              firstStartDate = endDate;
              target = new ImmutablePair<>(new HoodieAvroRecord<>(hoodieKey, payloadCreation.createPayload(rewriteRecord)), firstStartDate);
            }
            updateChainIndex(originKey, new HoodieAvroRecord<>(hoodieKey, payloadCreation.createPayload(rewriteRecord)), null, false);
            continue;
          }
          baseFileReader.close();
          scanner.close();
          return new ImmutablePair<>(new HoodieAvroRecord<>(hoodieKey, payloadCreation.createPayload(rewriteRecord)), firstStartDate);
        }
      }
      baseFileReader.close();
    }

    // write remaining records in log files,refer to writeIncomingRecords
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> newRecordsItr =
        (keyToNewRecords instanceof ExternalSpillableMap)
            ? ((ExternalSpillableMap) keyToNewRecords).iterator()
            : keyToNewRecords.values().iterator();
    while (newRecordsItr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> hoodieRecord = newRecordsItr.next();
      if (writtenRecordKeys.contains(hoodieRecord.getRecordKey())) {
        continue;
      }
      BaseAvroPayload payload = (BaseAvroPayload) hoodieRecord.getData();
      if (payload.recordBytes.length == 0) {
        continue;
      }
      GenericRecord indexedRecord = HoodieAvroUtils.bytesToAvro(payload.recordBytes, schemaWithMeta);

      List<String> originKeyList =
          BucketIdentifier.getHashKeys(hoodieRecord.getKey(), this.indexKeyFields);
      String originKeyInFileGroup = String.join(",", originKeyList);
      if (!originKey.equals(originKeyInFileGroup)) {
        continue;
      }
      GenericRecord rewriteRecord =
          HoodieAvroUtils.rewriteRecordWithNewSchema(
              indexedRecord, schema, Collections.emptyMap());
      if (partition.equals("")) {
        String startDate = HoodieAvroUtils.getNestedFieldVal(rewriteRecord, writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN), false, false).toString();
        String endDate = HoodieAvroUtils.getNestedFieldVal(rewriteRecord, writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN), false, false).toString();
        if (lateDate.compareTo(startDate) < 0 || lateDate.compareTo(endDate) >= 0) {
          firstStartDate = firstStartDate.compareTo(startDate) < 0 ? firstStartDate : startDate;
        } else {
          firstStartDate = endDate;
          target = new ImmutablePair<>(new HoodieAvroRecord<>(hoodieRecord.getKey(), payloadCreation.createPayload(rewriteRecord)), firstStartDate);
        }
        updateChainIndex(originKey, new HoodieAvroRecord<>(hoodieRecord.getKey(), payloadCreation.createPayload(rewriteRecord)), null, false);
        continue;
      }
      scanner.close();
      return new ImmutablePair<>(new HoodieAvroRecord<>(hoodieRecord.getKey(), payloadCreation.createPayload(rewriteRecord)), firstStartDate);
    }
    scanner.close();
    return target == null ? new ImmutablePair<>(null, firstStartDate) : target;
  }

  private void processLateRecord(HoodieRecord<?> record, String partition, int bucketNum, String originKey, String lateDate, String oldEndDate) throws Exception {
    if (partition.equals("")) {
      // find proper separating record in current bucket in nonPartitioned table
      Pair<HoodieRecord<?>, String> pair = getRecordOfOriginKey(partition, bucketNum, originKey, lateDate, oldEndDate);
      HoodieRecord<?> separatingRecord = pair.getLeft();
      if (separatingRecord == null) {
        LOG.info(String.format("Cannot find record: %s, originKey: %s in partition: %s, bucket: %s", record, originKey, partition, bucketNum));
        HoodieRecordLocation closeChainLocation = getCloseChainLocation(pair.getRight(), partition, bucketNum);
        addCloseChainRecord(record, partition, pair.getRight(), closeChainLocation);
        return;
      }
      String oldStartDate = BucketIdentifier.getHashKeys(separatingRecord.getKey(), writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN)).get(0);
      if (lateDate.compareTo(oldStartDate) > 0) {
        LOG.info(String.format("The proper separating record found: %s, late date: %s", separatingRecord, lateDate));
        // separate the record to two close chain record
        HoodieRecordLocation closeChainLocation = getCloseChainLocation(lateDate, partition, bucketNum);
        addCloseChainRecord(separatingRecord, partition, lateDate, closeChainLocation);
        closeChainLocation = getCloseChainLocation(pair.getRight(), partition, bucketNum);
        addCloseChainRecord(record, partition, pair.getRight(), closeChainLocation);
      } else {
        LOG.info(String.format("The proper separating record found: %s, late date: %s and the date is same", separatingRecord, lateDate));
        HoodieRecordPayload<?> recordPayload = (HoodieRecordPayload<?>) record.getData();
        BaseAvroPayload oldPayload = (BaseAvroPayload) separatingRecord.getData();
        GenericRecord oldIndexedRecord = HoodieAvroUtils.bytesToAvro(oldPayload.recordBytes, schema);
        GenericRecord mergedIndexedRecord = (GenericRecord) recordPayload.combineAndGetUpdateValue(oldIndexedRecord, schema, StreamerUtil.getPayloadConfig(config).getProps()).get();
        if (mergedIndexedRecord != oldIndexedRecord) {
          HoodieRecordLocation closeChainLocation = getCloseChainLocation(pair.getRight(), partition, bucketNum);
          addCloseChainRecord(record, partition, pair.getRight(), closeChainLocation);
        }
      }
    } else {
      // find proper separating record
      Pair<HoodieRecord<?>, String> pair = getRecordOfOriginKey(oldEndDate, bucketNum, originKey, lateDate, oldEndDate);
      HoodieRecord<?> separatingRecord = pair.getLeft();
      if (separatingRecord == null) {
        LOG.info(String.format("Cannot find record: %s, originKey: %s in partition: %s, bucket: %s", record, originKey, oldEndDate, bucketNum));
        HoodieRecordLocation closeChainLocation = getCloseChainLocation(pair.getRight(), partition, bucketNum);
        addCloseChainRecord(record, partition, pair.getRight(), closeChainLocation);
        return;
      }
      String oldStartDate = BucketIdentifier.getHashKeys(separatingRecord.getKey(), writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN)).get(0);
      if (lateDate.compareTo(oldStartDate) > 0) {
        LOG.info(String.format("The proper separating record found: %s, late date: %s", separatingRecord, lateDate));
        // separate the record to two close chain record
        HoodieRecordLocation closeChainLocation = getCloseChainLocation(lateDate, partition, bucketNum);
        addCloseChainRecord(separatingRecord, partition, lateDate, closeChainLocation);
        closeChainLocation = getCloseChainLocation(pair.getRight(), partition, bucketNum);
        addCloseChainRecord(record, partition, pair.getRight(), closeChainLocation);
        // delete the separating record
        String deleteRecordPartition =
            config.getBoolean(HIVE_STYLE_PARTITIONING) ? writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN) + "=" + pair.getRight() : pair.getRight();
        HoodieRecord<?> deleteRecord =
            new HoodieAvroRecord<>(
                new HoodieKey(
                    separatingRecord.getRecordKey(), deleteRecordPartition),
                payloadCreation.createDeletePayload((BaseAvroPayload) separatingRecord.getData()));
        deleteRecord.unseal();
        deleteRecord.setCurrentLocation(closeChainLocation);
        deleteRecord.seal();
        bufferRecord(deleteRecord);
      } else if (lateDate.compareTo(oldStartDate) < 0) {
        LOG.info(String.format("In partition: %s the original pk: %s startDate: %s is bigger than lateDate: %s", oldEndDate, originKey, oldStartDate, lateDate));
        updateChainIndex(originKey, separatingRecord, null, false);
        processLateRecord(record, partition, bucketNum, originKey, lateDate, oldStartDate);
      } else {
        LOG.info(String.format("The proper separating record found: %s, late date: %s and the date is same", separatingRecord, lateDate));
        HoodieRecordPayload<?> recordPayload = (HoodieRecordPayload<?>) record.getData();
        BaseAvroPayload oldPayload = (BaseAvroPayload) separatingRecord.getData();
        GenericRecord oldIndexedRecord = HoodieAvroUtils.bytesToAvro(oldPayload.recordBytes, schema);
        GenericRecord mergedIndexedRecord = (GenericRecord) recordPayload.combineAndGetUpdateValue(oldIndexedRecord, schema, StreamerUtil.getPayloadConfig(config).getProps()).get();
        if (mergedIndexedRecord != oldIndexedRecord) {
          HoodieRecordLocation closeChainLocation = getCloseChainLocation(pair.getRight(), partition, bucketNum);
          addCloseChainRecord(record, partition, pair.getRight(), closeChainLocation);
        }
      }
    }
  }

  private void deleteOldRecord(HoodieRecord<?> record, HoodieRecord<?> oldHoodieRecord, String partition) throws Exception {
    if (!partition.equals("")) {
      // delete oldStartDate record in 2999 partition
      deleteRecord(record, oldHoodieRecord);
    }
  }

  private void deleteRecord(HoodieRecord<?> record, HoodieRecord<?> oldHoodieRecord) throws Exception {
    HoodieRecord<?> deleteRecord =
        new HoodieAvroRecord<>(
            oldHoodieRecord.getKey(),
            payloadCreation.createDeletePayload((BaseAvroPayload) oldHoodieRecord.getData()));
    deleteRecord.unseal();
    deleteRecord.setCurrentLocation(record.getCurrentLocation());
    deleteRecord.seal();
    bufferRecord(deleteRecord);
  }

  private HoodieRecordLocation getCloseChainLocation(String date, String partition, int bucketNum) {
    // bootstrap new_start_date partition
    final HoodieRecordLocation closeChainLocation;
    final String closeBucketId;
    Map<Integer, String> closeBucketToFileId;
    if (!partition.equals("")) {
      String closeChainPartiton = config.getBoolean(HIVE_STYLE_PARTITIONING) ? writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN) + "=" + date : date;
      closeBucketToFileId = bucketIndex.computeIfAbsent(closeChainPartiton, p -> new HashMap<>());
      closeBucketId = closeChainPartiton + bucketNum;
    } else {
      closeBucketToFileId = bucketIndex.computeIfAbsent(partition, p -> new HashMap<>());
      closeBucketId = "" + bucketNum;
    }

    if (incBucketIndex.contains(closeBucketId)) {
      closeChainLocation = new HoodieRecordLocation("I", closeBucketToFileId.get(bucketNum));
    } else if (closeBucketToFileId.containsKey(bucketNum)) {
      closeChainLocation = new HoodieRecordLocation("U", closeBucketToFileId.get(bucketNum));
    } else {
      String newFileId = BucketIdentifier.newBucketFileIdPrefix(bucketNum);
      closeChainLocation = new HoodieRecordLocation("I", newFileId);
      closeBucketToFileId.put(bucketNum, newFileId);
      incBucketIndex.add(closeBucketId);
    }
    return closeChainLocation;
  }

  private void addCloseChainRecord(HoodieRecord<?> oldHoodieRecord, String partition, String newStartDate, HoodieRecordLocation closeChainLocation) throws Exception {
    // add a new close chain record in newStartDate partition
    BaseAvroPayload oldPayload = (BaseAvroPayload) oldHoodieRecord.getData();
    GenericRecord oldIndexedRecord = HoodieAvroUtils.bytesToAvro(oldPayload.recordBytes, schema);
    oldIndexedRecord.put(
        writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN),
        (int) LocalDate.parse(newStartDate).toEpochDay());
    String closeChainRecordPartition;
    if (partition.equals("")) {
      closeChainRecordPartition = "";
    } else {
      closeChainRecordPartition = config.getBoolean(HIVE_STYLE_PARTITIONING) ? writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN) + "=" + newStartDate : newStartDate;
    }
    HoodieRecord<?> closeChainRecord =
        new HoodieAvroRecord<>(
            new HoodieKey(
                oldHoodieRecord.getRecordKey(), closeChainRecordPartition),
            payloadCreation.createPayload(oldIndexedRecord));
    closeChainRecord.unseal();
    closeChainRecord.setCurrentLocation(closeChainLocation);
    closeChainRecord.seal();
    bufferRecord(closeChainRecord);
    List<String> originKeyList =
        BucketIdentifier.getHashKeys(closeChainRecord.getKey(), this.indexKeyFields);
    String originKey = String.join(",", originKeyList);
    updateChainIndex(originKey, closeChainRecord, null, false);
  }

  private void addMergedOpenChainRecord(HoodieRecord<?> record, HoodieRecord<?> oldHoodieRecord, String originKey) throws Exception {
    // startDate is the same, add newStartDate record in 2999 partition
    HoodieRecordPayload<?> recordPayload = (HoodieRecordPayload<?>) record.getData();
    BaseAvroPayload oldPayload = (BaseAvroPayload) oldHoodieRecord.getData();
    GenericRecord oldIndexedRecord = HoodieAvroUtils.bytesToAvro(oldPayload.recordBytes, schema);
    GenericRecord mergedIndexedRecord =
        (GenericRecord)
            recordPayload
                .combineAndGetUpdateValue(oldIndexedRecord, schema, StreamerUtil.getPayloadConfig(config).getProps())
                .get();
    HoodieRecord<?> mergedRecord =
        new HoodieAvroRecord<>(
            record.getKey(), payloadCreation.createPayload(mergedIndexedRecord));
    bufferRecord(record);
    updateChainIndex(originKey, mergedRecord, null, true);
  }

  private void updateOpenChainMap(String originKey, HoodieRecord<?> record, String endDate) throws Exception {
    if (endDate.equals(FlinkOptions.CHAIN_LATEST_PARTITION)) {
      openChainRecords.put(originKey, record);
      return;
    }
    HoodieRecord<?> oldRecord = openChainRecords.get(originKey);
    if (oldRecord == null) {
      String recordKey = record.getRecordKey();
      String newRecordKey = recordKey.substring(0, recordKey.lastIndexOf(writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN)))
          + writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN) + ":" + endDate;
      HoodieRecord<?> deleteRecord =
          new HoodieAvroRecord<>(new HoodieKey(newRecordKey, record.getPartitionPath()), payloadCreation.createDeletePayload((BaseAvroPayload) record.getData()));
      openChainRecords.put(originKey, deleteRecord);
    } else {
      String oldStartDate = BucketIdentifier.getHashKeys(oldRecord.getKey(), writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN)).get(0);
      if (endDate.compareTo(oldStartDate) > 0) {
        String recordKey = record.getRecordKey();
        String newRecordKey = recordKey.substring(0, recordKey.lastIndexOf(writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN)))
            + writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN) + ":" + endDate;
        HoodieRecord<?> deleteRecord =
            new HoodieAvroRecord<>(new HoodieKey(newRecordKey, record.getPartitionPath()), payloadCreation.createDeletePayload((BaseAvroPayload) record.getData()));
        openChainRecords.put(originKey, deleteRecord);
      }
    }
  }

  private void doChain(HoodieRecord<?> record, HoodieRecord<?> oldHoodieRecord, String partition, String oldStartDate)
      throws Exception {
    int bucketNum =
        BucketIdentifier.getBucketId(record.getKey(), this.indexKeyFields, this.bucketNum);
    List<String> originKeyList = BucketIdentifier.getHashKeys(record.getKey(), this.indexKeyFields);
    String originKey = String.join(",", originKeyList);
    oldStartDate = oldStartDate != null ? oldStartDate :
        BucketIdentifier.getHashKeys(
                oldHoodieRecord.getKey(),
                writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN))
            .get(0);
    String newStartDate =
        BucketIdentifier.getHashKeys(
                record.getKey(),
                writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN))
            .get(0);
    HoodieOperation operation = record.getOperation();
    // if oldHoodieRecord is chain closed, only process late record
    if (oldHoodieRecord == null || ((BaseAvroPayload) oldHoodieRecord.getData()).recordBytes.length == 0) {
      LOG.warn(String.format("The coming record's corresponding oldHoodieRecord is chain closed, record: %s oldRecord's chain close date: %s", record, oldStartDate));
      if (newStartDate.compareTo(oldStartDate) < 0 && !operation.equals(HoodieOperation.DELETE)) {
        LOG.warn("The coming record is late, it's corresponding oldHoodieRecord is chain closed");
        // late logic
        updateChainIndex(originKey, oldHoodieRecord, oldStartDate, false);
        processLateRecord(record, partition, bucketNum, originKey, newStartDate, oldStartDate);
      } else if (newStartDate.compareTo(oldStartDate) < 0 && operation.equals(HoodieOperation.DELETE)) {
        LOG.warn("The coming record is a late close chain record, it's corresponding oldHoodieRecord is chain closed");
      } else if (newStartDate.equals(oldStartDate)) {
        LOG.warn("The coming record's corresponding oldHoodieRecord is chain closed and their date is the same, ignore it");
      } else {
        LOG.warn("The coming record is newer, but it's corresponding oldHoodieRecord is chain closed, ignore it");
      }
      return;
    }

    // process new coming chain close record
    if (operation.equals(HoodieOperation.DELETE)) {
      if (newStartDate.compareTo(oldStartDate) < 0) {
        // the coming record is a close chain record and less than the oldStartDate, ignore it
        LOG.warn(String.format("The coming record is a close chain record and less than the oldStartDate, ignore it %s", record));
      } else if (newStartDate.equals(oldStartDate)) {
        // delete oldStartDate record both in partitioned and nonPartitioned table
        deleteRecord(record, oldHoodieRecord);
        HoodieRecord<?> deleteRecord =
            new HoodieAvroRecord<>(
                record.getKey(),
                payloadCreation.createDeletePayload((BaseAvroPayload) record.getData()));
        updateChainIndex(originKey, deleteRecord, null, true);
      } else {
        // delete oldStartDate record in 2999 partition if partitioned table
        deleteOldRecord(record, oldHoodieRecord, partition);
        // get close chain location
        HoodieRecordLocation closeChainLocation = getCloseChainLocation(newStartDate, partition, bucketNum);
        // add a new close chain record
        addCloseChainRecord(oldHoodieRecord, partition, newStartDate, closeChainLocation);
        HoodieRecord<?> deleteRecord =
            new HoodieAvroRecord<>(
                record.getKey(),
                payloadCreation.createDeletePayload((BaseAvroPayload) record.getData()));
        updateChainIndex(originKey, deleteRecord, null, true);
      }
      return;
    }

    if (newStartDate.equals(oldStartDate)) {
      // add merged open chain record
      addMergedOpenChainRecord(record, oldHoodieRecord, originKey);
    } else if (newStartDate.compareTo(oldStartDate) > 0) {
      // add newStartDate record in 2999 partition
      bufferRecord(record);
      updateChainIndex(originKey, record, null, true);
      // delete oldStartDate record in 2999 partition if partitioned table
      deleteOldRecord(record, oldHoodieRecord, partition);
      // get close chain location
      HoodieRecordLocation closeChainLocation = getCloseChainLocation(newStartDate, partition, bucketNum);
      // add a new close chain record
      addCloseChainRecord(oldHoodieRecord, partition, newStartDate, closeChainLocation);
    } else {
      // the coming record is late, process it
      LOG.warn(String.format("The coming record: %s is late", record));
      updateChainIndex(originKey, oldHoodieRecord, null, false);
      processLateRecord(record, partition, bucketNum, originKey, newStartDate, oldStartDate);
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
    try {
      this.writeConfig = FlinkWriteClients.getHoodieClientConfig(config);
      this.payloadCreation = PayloadCreation.instance(config);
      this.schema = new Schema.Parser().parse(writeConfig.getSchema());
      keyGeneratorOpt =
          Option.of(
              (BaseKeyGenerator)
                  HoodieAvroKeyGeneratorFactory.createKeyGenerator(
                      new TypedProperties(writeConfig.getProps())));
      unSentRecords = new HashMap<>();
      if (config.getString(CHAIN_SEARCH_MODE).equals(ChainedTableSearchMode.SPILL_MAP.name())) {
        this.openChainRecords =
            new ExternalSpillableMap<>(
                StreamerUtil.getMaxChainMemoryInBytes(config),
                writeConfig.getSpillableMapBasePath(),
                new DefaultSizeEstimator<>(),
                new DefaultSizeEstimator<>(),
                writeConfig.getCommonConfig().getSpillableDiskMapType(),
                writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled());
      } else {
        unCheckedRecords = new ArrayList<>();
        synchronized (ChainedBucketStreamWriteFunction.class) {
          if (hbaseConnection == null || hbaseConnection.isClosed()) {
            hbaseConnection = getHBaseConnection();
          }
        }
      }
    } catch (Exception ex) {
      throw new HoodieException("Cannot instantiate ChainedBucketStreamWrite", ex);
    }
  }

  @Override
  public void updateCurrentInstant() {
    super.updateCurrentInstant();
    if (config.getString(CHAIN_SEARCH_MODE).equals(ChainedTableSearchMode.HBASE.name())) {
      updateChainHbaseIndex();
      this.metaClient.reloadActiveTimeline();
      List<HoodieInstant> instants = this.metaClient.getActiveTimeline().getInstants().collect(toList());
      instants.add(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, this.currentInstant));
      this.metaClient.getActiveTimeline().setInstants(instants);
    }
  }

  @Override
  public void snapshotState() {
    if (config.getString(CHAIN_SEARCH_MODE).equals(ChainedTableSearchMode.HBASE.name())) {
      processUnCheckedRecords();
    }
    super.snapshotState();
    unSentRecords.clear();
  }

  @Override
  public void endInput() {
    if (config.getString(CHAIN_SEARCH_MODE).equals(ChainedTableSearchMode.HBASE.name())) {
      processUnCheckedRecords();
    }
    super.endInput();
  }

  @Override
  public void processElement(
      I i, ProcessFunction<I, Object>.Context context, Collector<Object> collector)
      throws Exception {
    HoodieRecord<?> record = (HoodieRecord<?>) i;
    final HoodieKey hoodieKey = record.getKey();
    final String partition = hoodieKey.getPartitionPath();
    final HoodieRecordLocation location;

    bootstrapIndexIfNeed(partition);
    Map<Integer, String> bucketToFileId =
        bucketIndex.computeIfAbsent(partition, p -> new HashMap<>());
    final int bucketNum =
        BucketIdentifier.getBucketId(hoodieKey, this.indexKeyFields, this.bucketNum);
    final String bucketId = partition + bucketNum;

    if (incBucketIndex.contains(bucketId)) {
      location = new HoodieRecordLocation("I", bucketToFileId.get(bucketNum));
    } else if (bucketToFileId.containsKey(bucketNum)) {
      location = new HoodieRecordLocation("U", bucketToFileId.get(bucketNum));
    } else {
      String newFileId = BucketIdentifier.newBucketFileIdPrefix(bucketNum);
      location = new HoodieRecordLocation("I", newFileId);
      bucketToFileId.put(bucketNum, newFileId);
      incBucketIndex.add(bucketId);
    }
    record.unseal();
    record.setCurrentLocation(location);
    record.seal();

    // When use spill_map, process each record at once, when use hbase, buffer the records to reduce
    // hbase query
    if (config.getString(CHAIN_SEARCH_MODE).equals(ChainedTableSearchMode.SPILL_MAP.name())) {
      // get original pk
      List<String> originKeyList = BucketIdentifier.getHashKeys(hoodieKey, this.indexKeyFields);
      String originKey = String.join(",", originKeyList);
      if (openChainRecords.containsKey(originKey)) {
        HoodieRecord<?> oldHoodieRecord = openChainRecords.get(originKey);
        doChain(record, oldHoodieRecord, partition, null);
      } else {
        processNewRecord(record, originKey);
      }
    } else {
      bufferUnCheckedRecords(record);
    }
  }

  /**
   * Get partition_bucket -> fileID mapping from the existing hudi table. This is a required
   * operation for each restart to avoid having duplicate file ids for one bucket.
   */
  @Override
  public void bootstrapIndexIfNeed(String partition) {
    // if partitioned table, should process all partitions as to bootstrap cancellation record
    List<String> partitions = new ArrayList<>();
    if (bucketIndex.containsKey(partition)) {
      return;
    }
    LOG.info(
        String.format(
            "Loading Hoodie Table %s, with path %s",
            this.metaClient.getTableConfig().getTableName(),
            this.metaClient.getBasePathV2() + "/" + partition));

    // Load existing fileID belongs to this task
    TableFileSystemView fsView = this.writeClient.getHoodieTable().getHoodieView();
    if (!fsView.getLastInstant().isPresent()) {
      LOG.info("No instant completed now, exit bootstrapIndexIfNeed.");
      return;
    }
    String latestCommit = fsView.getLastInstant().get().getTimestamp();
    if (!(fsView instanceof TableFileSystemView.SliceViewWithLatestSlice)) {
      throw new RuntimeException(
          "fsView is not a instance of TableFileSystemView.SliceViewWithLatestSlice");
    }

    if (partition.equals("")) {
      partitions.add(partition);
    } else {
      // get all partitions in the table
      HoodieMetadataConfig metadataConfig =
          HoodieMetadataConfig.newBuilder()
              .enable(config.getBoolean(METADATA_ENABLED))
              .withMetadataIndexColumnStats(writeConfig.getBoolean(ENABLE_METADATA_INDEX_COLUMN_STATS))
              .build();

      List<String> allPartitions = FSUtils.getAllPartitionPaths(HoodieFlinkEngineContext.DEFAULT, metadataConfig, writeConfig.getBasePath());
      partitions.addAll(allPartitions);
    }

    partitions.forEach(par -> {
      Map<Integer, String> bucketToFileIDMap = new HashMap<>();
      ((TableFileSystemView.SliceViewWithLatestSlice) fsView)
          .getLatestMergedFileSlicesBeforeOrOn(par, latestCommit)
          .forEach(
              fileSlice -> {
                String fileID = fileSlice.getFileId();
                int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileID);
                // use LATEST_PARTITION in partitioned table as the same bucket in other partitions
                // must be written in the same task
                String keyByPartiton;
                if (par.equals("")) {
                  keyByPartiton = "";
                } else {
                  keyByPartiton = config.getBoolean(HIVE_STYLE_PARTITIONING)
                      ? writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN) + "=" + FlinkOptions.CHAIN_LATEST_PARTITION : FlinkOptions.CHAIN_LATEST_PARTITION;
                }
                if (isBucketToLoad(bucketNumber, keyByPartiton)) {
                  LOG.info(
                      String.format(
                          "Should load this partition bucket %s with fileID %s",
                          bucketNumber, fileID));
                  if (bucketToFileIDMap.containsKey(bucketNumber)) {
                    throw new RuntimeException(
                        String.format(
                            "Duplicate fileID %s from bucket %s of partition %s found "
                                + "during the BucketStreamWriteFunction index bootstrap.",
                            fileID, bucketNumber, par));
                  } else {
                    LOG.info(
                        String.format(
                            "Adding fileID %s to the bucket %s of partition %s.",
                            fileID, bucketNumber, par));
                    bucketToFileIDMap.put(bucketNumber, fileID);
                    // get bucket data in this fileSlice and put it to the ExternalSpillableMap
                    getRecordsInBucket(fileSlice);
                    }
                  }
              });
      bucketIndex.put(par, bucketToFileIDMap);
    });
  }

  @Override
  public void close() {
    super.close();
    if (config.getString(CHAIN_SEARCH_MODE).equals(ChainedTableSearchMode.SPILL_MAP.name())) {
      openChainRecords.close();
    } else {
      synchronized (ChainedBucketStreamWriteFunction.class) {
        if (hbaseConnection != null && !hbaseConnection.isClosed()) {
          try {
            hbaseConnection.close();
            hbaseConnection = null;
          } catch (Exception e) {
            LOG.error("Hbase connection close failed");
          }
        }
      }
    }
  }

  private void getRecordsInBucket(FileSlice latestFileSlice) {
    if (!config.getString(CHAIN_SEARCH_MODE).equals(ChainedTableSearchMode.SPILL_MAP.name()) || latestFileSlice == null) {
      return;
    }

    HoodieBaseFile baseFile;
    HoodieFileReader<GenericRecord> baseFileReader;
    Iterator<GenericRecord> baseReaderIterator;

    Schema schemaWithMeta = schema;
    if (metaClient.getTableConfig().populateMetaFields()) {
      schemaWithMeta =
          HoodieAvroUtils.addMetadataFields(schema, writeConfig.allowOperationMetadataField());
    }

    // generate base file reader
    baseFile = latestFileSlice.getBaseFile().orElse(null);

    // generate log files reader
    List<String> logFiles =
        latestFileSlice
            .getLogFiles()
            .sorted(HoodieLogFile.getLogFileComparator())
            .map(logFile -> logFile.getPath().toString())
            .collect(toList());
    String maxInstantTime =
        metaClient
            .getActiveTimeline()
            .getTimelineOfActions(
                CollectionUtils.createSet(
                    HoodieTimeline.COMMIT_ACTION,
                    HoodieTimeline.ROLLBACK_ACTION,
                    HoodieTimeline.DELTA_COMMIT_ACTION))
            .filterCompletedInstants()
            .lastInstant()
            .get()
            .getTimestamp();

    HoodieMergedLogRecordScanner scanner =
        HoodieMergedLogRecordScanner.newBuilder()
            .withFileSystem(metaClient.getFs())
            .withBasePath(metaClient.getBasePathV2().toString())
            .withLogFilePaths(logFiles)
            .withReaderSchema(schemaWithMeta)
            .withLatestInstantTime(maxInstantTime)
            .withInternalSchema(
                SerDeHelper.fromJson(writeConfig.getInternalSchema())
                    .orElse(InternalSchema.getEmptyInternalSchema()))
            .withMaxMemorySizeInBytes(StreamerUtil.getMaxCompactionMemoryInBytes(config))
            //        .withReadBlocksLazily(writeConfig.getCompactionLazyBlockReadEnabled())
            .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
            .withSpillableMapBasePath(writeConfig.getSpillableMapBasePath())
            .withDiskMapType(writeConfig.getCommonConfig().getSpillableDiskMapType())
            .withBitCaskDiskMapCompressionEnabled(
                writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
            .withOperationField(writeConfig.allowOperationMetadataField())
            .build();

    Map<String, HoodieRecord<? extends HoodieRecordPayload>> keyToNewRecords = scanner.getRecords();
    Set<String> writtenRecordKeys = new HashSet<>();

    // write records in base file
    if (baseFile != null) {
      Path baseFilePath = new Path(baseFile.getPath());
      HoodieKey hoodieKey;
      try {
        baseFileReader =
            HoodieFileReaderFactory.getFileReader(
                new org.apache.hadoop.conf.Configuration(), baseFilePath);
        baseReaderIterator = baseFileReader.getRecordIterator();
      } catch (Exception ex) {
        throw new HoodieException("Open baseFileReader error");
      }

      while (baseReaderIterator.hasNext()) {
        GenericRecord baseRecord = baseReaderIterator.next();
        String key = KeyGenUtils.getRecordKeyFromGenericRecord(baseRecord, keyGeneratorOpt);
        writtenRecordKeys.add(key);
        String partition =
            KeyGenUtils.getPartitionPathFromGenericRecord(baseRecord, keyGeneratorOpt);
        hoodieKey = new HoodieKey(key, partition);
        // get original pk
        List<String> originKeyList =
            BucketIdentifier.getHashKeys(hoodieKey, this.indexKeyFields);
        String originKey = String.join(",", originKeyList);
        if (keyToNewRecords.containsKey(key)) {
          HoodieRecord<? extends HoodieRecordPayload> hoodieRecord =
              keyToNewRecords.get(key).newInstance();
          try {
            Option<IndexedRecord> combinedAvroRecord =
                hoodieRecord
                    .getData()
                    .combineAndGetUpdateValue(
                        baseRecord,
                        schemaWithMeta,
                        StreamerUtil.getPayloadConfig(config).getProps());
            if (combinedAvroRecord.isPresent()) {
              GenericRecord record = (GenericRecord) combinedAvroRecord.get();
              GenericRecord rewriteRecord =
                  HoodieAvroUtils.rewriteRecordWithNewSchema(
                      record, schema, Collections.emptyMap());
              HoodieRecord<?> combinedRecord =
                  new HoodieAvroRecord<>(hoodieKey, payloadCreation.createPayload(rewriteRecord));
              String endDate =
                  HoodieAvroUtils.getNestedFieldVal(record, writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN), false, false).toString();
              updateOpenChainMap(originKey, combinedRecord, endDate);
            }
          } catch (Exception ex) {
            throw new HoodieException("Merge hoodie record error");
          }
        } else {
          try {
            GenericRecord rewriteRecord =
                HoodieAvroUtils.rewriteRecordWithNewSchema(
                    baseRecord, schema, Collections.emptyMap());
            HoodieRecord<?> rewriteBaseRecord =
                new HoodieAvroRecord<>(hoodieKey, payloadCreation.createPayload(rewriteRecord));
            String endDate =
                HoodieAvroUtils.getNestedFieldVal(rewriteRecord, writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN), false, false).toString();
            updateOpenChainMap(originKey, rewriteBaseRecord, endDate);
          } catch (Exception ex) {
            throw new HoodieException("Rewrite BaseRecord error.");
          }
        }
      }
      baseFileReader.close();
    }

    // write remaining records in log files,refer to writeIncomingRecords
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> newRecordsItr =
        (keyToNewRecords instanceof ExternalSpillableMap)
            ? ((ExternalSpillableMap) keyToNewRecords).iterator()
            : keyToNewRecords.values().iterator();
    while (newRecordsItr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> hoodieRecord = newRecordsItr.next();
      if (!writtenRecordKeys.contains(hoodieRecord.getRecordKey())) {
        BaseAvroPayload payload = (BaseAvroPayload) hoodieRecord.getData();
        if (payload.recordBytes.length == 0) {
          continue;
        }
        GenericRecord indexedRecord;
        try {
          indexedRecord = HoodieAvroUtils.bytesToAvro(payload.recordBytes, schemaWithMeta);
        } catch (Exception ex) {
          throw new HoodieException("Record bytesToAvro error:", ex);
        }

        List<String> originKeyList =
            BucketIdentifier.getHashKeys(hoodieRecord.getKey(), this.indexKeyFields);
        String originKey = String.join(",", originKeyList);
        GenericRecord rewriteRecord =
            HoodieAvroUtils.rewriteRecordWithNewSchema(
                indexedRecord, schema, Collections.emptyMap());
        try {
          HoodieRecord<?> rewriteHoodieRecord =
              new HoodieAvroRecord<>(
                  hoodieRecord.getKey(), payloadCreation.createPayload(rewriteRecord));
          String endDate =
              HoodieAvroUtils.getNestedFieldVal(rewriteRecord, writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN), false, false).toString();
          updateOpenChainMap(originKey, rewriteHoodieRecord, endDate);
        } catch (Exception ex) {
          throw new HoodieException("rewrite hoodie record error");
        }
      }
    }
    scanner.close();
  }
}
