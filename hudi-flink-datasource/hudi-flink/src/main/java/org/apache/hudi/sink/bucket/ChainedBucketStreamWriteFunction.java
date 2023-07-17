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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.stream.Collectors.toList;

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

  /**
   * Chain table's original pk to HoodieRecord mapping in 2999 partition. Map(original pk ->
   * HoodieRecord).
   */
  private transient ExternalSpillableMap<String, HoodieRecord<?>> openChainRecords;

  private transient HoodieWriteConfig writeConfig;

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

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
    try {
      this.writeConfig = FlinkWriteClients.getHoodieClientConfig(config);
      this.payloadCreation = PayloadCreation.instance(config);
      this.openChainRecords =
          new ExternalSpillableMap<>(
              StreamerUtil.getMaxChainMemoryInBytes(config),
              writeConfig.getSpillableMapBasePath(),
              new DefaultSizeEstimator<>(),
              new DefaultSizeEstimator<>(),
              writeConfig.getCommonConfig().getSpillableDiskMapType(),
              writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled());
    } catch (Exception ex) {
      throw new HoodieException("Cannot instantiate an ExternalSpillableMap", ex);
    }
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
    // get original pk
    List<String> originKeyList = BucketIdentifier.getHashKeys(hoodieKey, this.indexKeyFields);
    String originKey = String.join(",", originKeyList);
    if (openChainRecords.containsKey(originKey)) {
      Schema schema = new Schema.Parser().parse(writeConfig.getSchema());
      HoodieRecord<?> oldHoodieRecord = openChainRecords.get(originKey);
      String oldStartDate =
          BucketIdentifier.getHashKeys(
                  oldHoodieRecord.getKey(),
                  writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN))
              .get(0);
      String newStartDate =
          BucketIdentifier.getHashKeys(
                  record.getKey(),
                  writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_START_DATE_COLUMN))
              .get(0);
      if (newStartDate.equals(oldStartDate)) {
        // startDate is the same, add newStartDate record in 2999 partition
        record.unseal();
        record.setCurrentLocation(location);
        record.seal();
        bufferRecord(record);
        HoodieRecordPayload<?> recordPayload = (HoodieRecordPayload<?>) record.getData();
        BaseAvroPayload oldPayload = (BaseAvroPayload) oldHoodieRecord.getData();
        GenericRecord oldIndexedRecord =
            HoodieAvroUtils.bytesToAvro(oldPayload.recordBytes, schema);
        GenericRecord mergedIndexedRecord =
            (GenericRecord)
                recordPayload
                    .combineAndGetUpdateValue(oldIndexedRecord, schema, new Properties())
                    .get();
        HoodieRecord<?> mergedRecord =
            new HoodieAvroRecord<>(hoodieKey, payloadCreation.createPayload(mergedIndexedRecord));
        openChainRecords.put(originKey, mergedRecord);
      } else if (newStartDate.compareTo(oldStartDate) > 0) {
        // bootstrap new_start_date partition
        final HoodieRecordLocation closeChainLocation;
        final String closeBucketId;
        Map<Integer, String> closeBucketToFileId;
        if (!partition.equals("")) {
          bootstrapIndexIfNeed(newStartDate);
          closeBucketToFileId = bucketIndex.computeIfAbsent(newStartDate, p -> new HashMap<>());
          closeBucketId = newStartDate + bucketNum;
        } else {
          closeBucketToFileId = bucketToFileId;
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
        // add newStartDate record in 2999 partition
        record.unseal();
        record.setCurrentLocation(location);
        record.seal();
        bufferRecord(record);
        openChainRecords.put(originKey, record);
        if (!partition.equals("")) {
          // delete oldStartDate record in 2999 partition
          HoodieRecord<?> deleteRecord =
              new HoodieAvroRecord<>(
                  oldHoodieRecord.getKey(),
                  payloadCreation.createDeletePayload((BaseAvroPayload) oldHoodieRecord.getData()));
          deleteRecord.unseal();
          deleteRecord.setCurrentLocation(location);
          deleteRecord.seal();
          bufferRecord(deleteRecord);
        }

        // add a new close chain record in newStartDate partition
        //        HoodieAvroUtils.getNestedFieldVal(record.getData(),
        // writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN), false,
        // false);
        BaseAvroPayload oldPayload = (BaseAvroPayload) oldHoodieRecord.getData();
        GenericRecord oldIndexedRecord =
            HoodieAvroUtils.bytesToAvro(oldPayload.recordBytes, schema);
        oldIndexedRecord.put(
            writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN),
            (int) LocalDate.parse(newStartDate).toEpochDay());
        HoodieRecord<?> closeChainRecord =
            new HoodieAvroRecord<>(
                new HoodieKey(
                    oldHoodieRecord.getRecordKey(),
                    partition.equals("") ? partition : newStartDate),
                payloadCreation.createPayload(oldIndexedRecord));
        closeChainRecord.unseal();
        closeChainRecord.setCurrentLocation(closeChainLocation);
        closeChainRecord.seal();
        bufferRecord(closeChainRecord);
      } else {
        // the coming record is late, ignore it
        LOG.warn(String.format("The coming record is late, ignore it %s", record));
      }
    } else {
      // the record's original pk is new
      record.unseal();
      record.setCurrentLocation(location);
      record.seal();
      bufferRecord(record);
      openChainRecords.put(originKey, record);
    }
  }

  /**
   * Get partition_bucket -> fileID mapping from the existing hudi table. This is a required
   * operation for each restart to avoid having duplicate file ids for one bucket.
   */
  @Override
  public void bootstrapIndexIfNeed(String partition) {
    if (bucketIndex.containsKey(partition)) {
      return;
    }
    LOG.info(
        String.format(
            "Loading Hoodie Table %s, with path %s",
            this.metaClient.getTableConfig().getTableName(),
            this.metaClient.getBasePathV2() + "/" + partition));

    // Load existing fileID belongs to this task
    Map<Integer, String> bucketToFileIDMap = new HashMap<>();
    this.writeClient
        .getHoodieTable()
        .getFileSystemView()
        .getAllFileGroups(partition)
        .forEach(
            fileGroup -> {
              String fileID = fileGroup.getFileGroupId().getFileId();
              int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileID);
              // use LATEST_PARTITION in partitioned table as the same bucket in other partitions
              // must be written in the same task
              String keyByPartiton =
                  partition.equals("") ? partition : FlinkOptions.CHAIN_LATEST_PARTITION;
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
                          fileID, bucketNumber, partition));
                } else {
                  LOG.info(
                      String.format(
                          "Adding fileID %s to the bucket %s of partition %s.",
                          fileID, bucketNumber, partition));
                  bucketToFileIDMap.put(bucketNumber, fileID);
                  // get bucket data in the latest partition and put it to the ExternalSpillableMap
                  if (partition.equals(FlinkOptions.CHAIN_LATEST_PARTITION)
                      || partition.equals("")) {
                    getRecordsInBucket(!partition.equals(""), fileGroup);
                  }
                }
              }
            });
    bucketIndex.put(partition, bucketToFileIDMap);
  }

  @Override
  public void close() {
    super.close();
    openChainRecords.close();
  }

  private void getRecordsInBucket(boolean partitionTable, HoodieFileGroup fileGroup) {
    FileSlice latestFileSlice = fileGroup.getLatestFileSlice().orElse(null);
    HoodieBaseFile baseFile;
    HoodieFileReader<GenericRecord> baseFileReader;
    Iterator<GenericRecord> baseReaderIterator;
    if (latestFileSlice == null) {
      return;
    }

    Schema schema = new Schema.Parser().parse(writeConfig.getSchema());
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
            .map(HoodieLogFile::getPath)
            .map(Path::toString)
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
    Option<BaseKeyGenerator> keyGeneratorOpt;

    // write records in base file
    if (baseFile != null) {
      Path baseFilePath = new Path(baseFile.getPath());
      HoodieKey hoodieKey;
      try {
        baseFileReader =
            HoodieFileReaderFactory.getFileReader(
                new org.apache.hadoop.conf.Configuration(), baseFilePath);
        baseReaderIterator = baseFileReader.getRecordIterator();
        keyGeneratorOpt =
            Option.of(
                (BaseKeyGenerator)
                    HoodieAvroKeyGeneratorFactory.createKeyGenerator(
                        new TypedProperties(writeConfig.getProps())));
      } catch (Exception ex) {
        throw new HoodieException("Open baseFileReader error");
      }

      while (baseReaderIterator.hasNext()) {
        GenericRecord baseRecord = baseReaderIterator.next();
        String key = KeyGenUtils.getRecordKeyFromGenericRecord(baseRecord, keyGeneratorOpt);
        String partition =
            KeyGenUtils.getPartitionPathFromGenericRecord(baseRecord, keyGeneratorOpt);
        if (!partitionTable) {
          String nestedFieldVal =
              (String)
                  HoodieAvroUtils.getNestedFieldVal(
                      baseRecord,
                      writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN),
                      false,
                      false);
          if (!nestedFieldVal.equals(FlinkOptions.CHAIN_LATEST_PARTITION)) {
            writtenRecordKeys.add(key);
            continue;
          }
        }
        hoodieKey = new HoodieKey(key, partition);
        if (keyToNewRecords.containsKey(key)) {
          HoodieRecord<? extends HoodieRecordPayload> hoodieRecord =
              keyToNewRecords.get(key).newInstance();
          try {
            Option<IndexedRecord> combinedAvroRecord =
                hoodieRecord
                    .getData()
                    .combineAndGetUpdateValue(
                        baseRecord,
                        new Schema.Parser().parse(writeConfig.getSchema()),
                        new Properties());
            if (combinedAvroRecord.isPresent()) {
              GenericRecord record = (GenericRecord) combinedAvroRecord.get();
              // get original pk
              List<String> originKeyList =
                  BucketIdentifier.getHashKeys(hoodieKey, this.indexKeyFields);
              String originKey = String.join(",", originKeyList);
              GenericRecord rewriteRecord =
                  HoodieAvroUtils.rewriteRecordWithNewSchema(
                      record, schema, Collections.emptyMap());
              HoodieRecord<?> combinedRecord =
                  new HoodieAvroRecord<>(hoodieKey, payloadCreation.createPayload(rewriteRecord));
              openChainRecords.put(originKey, combinedRecord);
            }
            writtenRecordKeys.add(key);
          } catch (Exception ex) {
            throw new HoodieException("Merge hoodie record error");
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
        GenericRecord indexedRecord;
        try {
          indexedRecord = HoodieAvroUtils.bytesToAvro(payload.recordBytes, schemaWithMeta);
        } catch (Exception ex) {
          throw new HoodieException("Record bytesToAvro error");
        }
        if (!partitionTable) {
          String nestedFieldVal =
              HoodieAvroUtils.getNestedFieldVal(
                      indexedRecord,
                      writeConfig.getStringOrDefault(HoodieWriteConfig.TABLE_CHAIN_END_DATE_COLUMN),
                      false,
                      false)
                  .toString();
          if (!nestedFieldVal.equals(FlinkOptions.CHAIN_LATEST_PARTITION)) {
            continue;
          }
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
          openChainRecords.put(originKey, rewriteHoodieRecord);
        } catch (Exception ex) {
          throw new HoodieException("rewrite hoodie record error");
        }
      }
    }
    scanner.close();
  }
}
