/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is responsible for calculating names and types of fields that are actual at a certain point in time.
 * If field is renamed in queried schema, its old name will be returned, which is relevant at the provided time.
 * If type of field is changed, its old type will be returned, and projection will be created that will convert the old type to the queried one.
 */
public final class SchemaEvolutionContext implements Serializable {
  private static final long serialVersionUID = 1L;

  private final HoodieTableMetaClient metaClient;
  private final InternalSchema querySchema;

  public static Option<SchemaEvolutionContext> of(Configuration conf) {
    if (conf.getBoolean(FlinkOptions.SCHEMA_EVOLUTION_ENABLED)) {
      HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
      return new TableSchemaResolver(metaClient)
          .getTableInternalSchemaFromCommitMetadata()
          .map(schema -> new SchemaEvolutionContext(metaClient, schema));
    } else {
      return Option.empty();
    }
  }

  public SchemaEvolutionContext(HoodieTableMetaClient metaClient, InternalSchema querySchema) {
    this.metaClient = metaClient;
    this.querySchema = querySchema;
  }

  public InternalSchema getQuerySchema() {
    return querySchema;
  }

  public InternalSchema getActualSchema(FileInputSplit fileSplit) {
    return getActualSchema(FSUtils.getCommitTime(fileSplit.getPath().getName()));
  }

  public InternalSchema getActualSchema(MergeOnReadInputSplit split) {
    String commitTime = split.getBasePath()
        .map(FSUtils::getCommitTime)
        .orElse(split.getLatestCommit());
    return getActualSchema(commitTime);
  }

  public List<String> getFieldNames(InternalSchema internalSchema) {
    return internalSchema.columns().stream().map(Types.Field::name).collect(Collectors.toList());
  }

  public List<DataType> getFieldTypes(InternalSchema internalSchema) {
    return AvroSchemaConverter.convertToDataType(
        AvroInternalSchemaConverter.convert(internalSchema, getTableName())).getChildren();
  }

  public CastMap getCastMap(InternalSchema querySchema, InternalSchema actualSchema) {
    return CastMap.of(getTableName(), querySchema, actualSchema);
  }

  public static LogicalType[] project(List<DataType> fieldTypes, int[] selectedFields) {
    return Arrays.stream(selectedFields)
        .mapToObj(pos -> fieldTypes.get(pos).getLogicalType())
        .toArray(LogicalType[]::new);
  }

  private InternalSchema getActualSchema(String commitTime) {
    InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(Long.parseLong(commitTime), metaClient, false);
    return new InternalSchemaMerger(fileSchema, getQuerySchema(), true, true).mergeSchema();
  }

  private String getTableName() {
    return metaClient.getTableConfig().getTableName();
  }
}
