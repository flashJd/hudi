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

package org.apache.hudi.table.format;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.sink.utils.StreamWriteFunctionWrapper;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.SchemaBuilder;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.utils.TestConfigurations.ROW_DATA_TYPE;
import static org.apache.hudi.utils.TestConfigurations.ROW_DATA_TYPE_ADD_SCORE;
import static org.apache.hudi.utils.TestConfigurations.ROW_DATA_TYPE_AGE_LONG;
import static org.apache.hudi.utils.TestConfigurations.ROW_DATA_TYPE_CHANGE_AGE_INT2DOUBLE;
import static org.apache.hudi.utils.TestConfigurations.ROW_DATA_TYPE_DEL_AGE;
import static org.apache.hudi.utils.TestConfigurations.ROW_TYPE;
import static org.apache.hudi.utils.TestConfigurations.set_row_type;
import static org.apache.hudi.utils.TestData.insertRow;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for MergeOnReadInputFormat and ParquetInputFormat.
 */
public class TestCustomInputFormat {

  private HoodieTableSource tableSource;
  private Configuration conf;

  File tempFile;

  void beforeEach(HoodieTableType tableType) throws IOException {
    beforeEach(tableType, Collections.emptyMap());
  }

  void beforeEach(HoodieTableType tableType, Map<String, String> options) throws IOException {
    tempFile = new File("./hudi_table");
    FileUtils.deleteDirectory(tempFile);
    tempFile.mkdirs();
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false); // close the async compaction
    conf.setBoolean(FlinkOptions.INDEX_GLOBAL_ENABLED, true);
    options.forEach((key, value) -> conf.setString(key, value));

    StreamerUtil.initTableIfNotExists(conf);
    this.tableSource = getTableSource(conf);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadAddColumnScore(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA.key(), AvroSchemaConverter.convertToSchema(ROW_DATA_TYPE.getLogicalType()).toString());

    beforeEach(tableType, options);
    conf.setBoolean(FlinkOptions.SCHEMA_EVOLUTION_ENABLED, true);
    conf.setBoolean(FlinkOptions.RECONCILE_SCHEMA, true);
    conf.removeConfig(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH);
    set_row_type(ROW_DATA_TYPE);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // write another commit to read again
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(ROW_DATA_TYPE_ADD_SCORE.getLogicalType()).toString());
    set_row_type(ROW_DATA_TYPE_ADD_SCORE);
    List<RowData> dataSetInsert = Arrays.asList(
            // new data
            insertRow(StringData.fromString("id9"), StringData.fromString("Jane"), 22,
                    TimestampData.fromEpochMillis(6), StringData.fromString("par3"), 22),
            insertRow(StringData.fromString("id10"), StringData.fromString("Ella"), 33,
                    TimestampData.fromEpochMillis(7), StringData.fromString("par4"), 33),
            insertRow(StringData.fromString("id1"), StringData.fromString("Phoebe"), 44,
                    TimestampData.fromEpochMillis(8), StringData.fromString("par1"), 44)
    );
    TestData.writeData(dataSetInsert, conf);

    // refresh the input format
    this.tableSource.reset();

    ResolvedSchema newSchema = SchemaBuilder.instance()
            .fields(((RowType) ROW_DATA_TYPE_ADD_SCORE.getLogicalType()).getFieldNames(), ROW_DATA_TYPE_ADD_SCORE.getChildren())
            .build();
    this.tableSource = new HoodieTableSource(
            newSchema,
            new Path(tempFile.getAbsolutePath()),
            Collections.singletonList("partition"),
            "default",
            conf);
    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();

    List<RowData> result = readData(inputFormat);

    String actual = TestData.rowDataToString(result, ROW_DATA_TYPE_ADD_SCORE);
    String expected = "["
            + "+I[id1, Phoebe, 44, 1970-01-01T00:00:00.008, par1, 44], "
            + "+I[id2, Stephen, 33, 1970-01-01T00:00:00.002, par1, null], "
            + "+I[id3, Julian, 53, 1970-01-01T00:00:00.003, par2, null], "
            + "+I[id4, Fabian, 31, 1970-01-01T00:00:00.004, par2, null], "
            + "+I[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3, null], "
            + "+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3, null], "
            + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4, null], "
            + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4, null], "
            + "+I[id9, Jane, 22, 1970-01-01T00:00:00.006, par3, 22], "
            + "+I[id10, Ella, 33, 1970-01-01T00:00:00.007, par4, 33]]";
    assertThat(actual, is(expected));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadDeleteColumnAge(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA.key(), AvroSchemaConverter.convertToSchema(ROW_DATA_TYPE.getLogicalType()).toString());
    beforeEach(tableType, options);

    conf.setBoolean(FlinkOptions.SCHEMA_EVOLUTION_ENABLED, true);
    conf.setBoolean(FlinkOptions.RECONCILE_SCHEMA, true);
    conf.removeConfig(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH);
    set_row_type(ROW_DATA_TYPE);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // write another commit to read again
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(ROW_DATA_TYPE_DEL_AGE.getLogicalType()).toString());
    set_row_type(ROW_DATA_TYPE_DEL_AGE);
    List<RowData> dataSetInsert = Arrays.asList(
        // new data
        insertRow(StringData.fromString("id9"), StringData.fromString("Jane"),
                  TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
        insertRow(StringData.fromString("id10"), StringData.fromString("Ella"),
                  TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
        insertRow(StringData.fromString("id1"), StringData.fromString("Phoebe"),
                  TimestampData.fromEpochMillis(8), StringData.fromString("par1"))
    );
    TestData.writeData(dataSetInsert, conf);

    // refresh the input format
    this.tableSource.reset();

    set_row_type(ROW_DATA_TYPE);
    ResolvedSchema newSchema = SchemaBuilder.instance()
        .fields(((RowType) ROW_DATA_TYPE.getLogicalType()).getFieldNames(), ROW_DATA_TYPE.getChildren())
        .build();
    this.tableSource = new HoodieTableSource(
            newSchema,
        new Path(tempFile.getAbsolutePath()),
        Collections.singletonList("partition"),
        "default",
        conf);
    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();

    List<RowData> result = readData(inputFormat);

    String actual = TestData.rowDataToString(result, ROW_DATA_TYPE);
    String expected;
    if (tableType == COPY_ON_WRITE) {
      expected = "["
              + "+I[id1, Phoebe, null, 1970-01-01T00:00:00.008, par1], "
              + "+I[id2, Stephen, null, 1970-01-01T00:00:00.002, par1], "
              + "+I[id3, Julian, 53, 1970-01-01T00:00:00.003, par2], "
              + "+I[id4, Fabian, 31, 1970-01-01T00:00:00.004, par2], "
              + "+I[id5, Sophia, null, 1970-01-01T00:00:00.005, par3], "
              + "+I[id6, Emma, null, 1970-01-01T00:00:00.006, par3], "
              + "+I[id7, Bob, null, 1970-01-01T00:00:00.007, par4], "
              + "+I[id8, Han, null, 1970-01-01T00:00:00.008, par4], "
              + "+I[id9, Jane, null, 1970-01-01T00:00:00.006, par3], "
              + "+I[id10, Ella, null, 1970-01-01T00:00:00.007, par4]]";
    } else {
      expected = "["
              + "+I[id1, Phoebe, null, 1970-01-01T00:00:00.008, par1], "
              + "+I[id2, Stephen, 33, 1970-01-01T00:00:00.002, par1], "
              + "+I[id3, Julian, 53, 1970-01-01T00:00:00.003, par2], "
              + "+I[id4, Fabian, 31, 1970-01-01T00:00:00.004, par2], "
              + "+I[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3], "
              + "+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3], "
              + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
              + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4], "
              + "+I[id9, Jane, null, 1970-01-01T00:00:00.006, par3], "
              + "+I[id10, Ella, null, 1970-01-01T00:00:00.007, par4]]";
    }

    assertThat(actual, is(expected));
  }

  @Test
  void testReadChangeColumnAgeInt2Double() throws Exception {
    Map<String, String> options = new HashMap<>();
    // compact for each commit
    options.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), "10");
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "true");
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH.key(),
            Objects.requireNonNull(Thread.currentThread()
                    .getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    beforeEach(HoodieTableType.MERGE_ON_READ, options);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    InputFormat<RowData, ?> inputFormat;

    List<RowData> result;
    String actual;
    String expected;

    // write another commit to read again
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH, Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("test_read_schema2.avsc")).toString());
    set_row_type(ROW_DATA_TYPE_CHANGE_AGE_INT2DOUBLE);
    List<RowData> dataSetInsert = Arrays.asList(
        // new data
        insertRow(StringData.fromString("id9"), StringData.fromString("Jane"), 22.2,
                  TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
        insertRow(StringData.fromString("id10"), StringData.fromString("Ella"), 33.3,
                  TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
        insertRow(StringData.fromString("id1"), StringData.fromString("Phoebe"), 44.4,
                  TimestampData.fromEpochMillis(8), StringData.fromString("par1"))
    );
    TestData.writeData(dataSetInsert, conf);

    this.tableSource.reset();

    ResolvedSchema newSchema = SchemaBuilder.instance()
        .fields(ROW_TYPE.getFieldNames(), ROW_DATA_TYPE_CHANGE_AGE_INT2DOUBLE.getChildren())
        .build();
    this.tableSource = new HoodieTableSource(
            newSchema,
        new Path(tempFile.getAbsolutePath()),
        Collections.singletonList("partition"),
        "default",
        conf);
    inputFormat = this.tableSource.getInputFormat();

    result = readData(inputFormat);

    actual = TestData.rowDataToString(result, ROW_DATA_TYPE_CHANGE_AGE_INT2DOUBLE);
    expected = "["
        + "+I[id1, Phoebe, 44.4, 1970-01-01T00:00:00.008, par1], "
        + "+I[id2, Stephen, 33.0, 1970-01-01T00:00:00.002, par1], "
        + "+I[id3, Julian, 53.0, 1970-01-01T00:00:00.003, par2], "
        + "+I[id4, Fabian, 31.0, 1970-01-01T00:00:00.004, par2], "
        + "+I[id5, Sophia, 18.0, 1970-01-01T00:00:00.005, par3], "
        + "+I[id6, Emma, 20.0, 1970-01-01T00:00:00.006, par3], "
        + "+I[id7, Bob, 44.0, 1970-01-01T00:00:00.007, par4], "
        + "+I[id8, Han, 56.0, 1970-01-01T00:00:00.008, par4], "
        + "+I[id9, Jane, 22.2, 1970-01-01T00:00:00.006, par3], "
        + "+I[id10, Ella, 33.3, 1970-01-01T00:00:00.007, par4]]";
    assertThat(actual, is(expected));
  }

  @Test
  void testReadChangeColumnAgeLong2int() throws Exception {
    Map<String, String> options = new HashMap<>();
    // compact for each commit
    options.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), "10");
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "true");
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH.key(),
            Objects.requireNonNull(Thread.currentThread()
                    .getContextClassLoader().getResource("test_read_schema_age_long.avsc")).toString());
    beforeEach(COPY_ON_WRITE, options);

    insertRow(StringData.fromString("id9"), StringData.fromString("Jane"), 22,
            TimestampData.fromEpochMillis(6), StringData.fromString("par3"));
    set_row_type(ROW_DATA_TYPE_AGE_LONG);
    List<RowData> dataSetInsert = Arrays.asList(
        // new data
        insertRow(StringData.fromString("id9"), StringData.fromString("Jane"), 22L,
                  TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
        insertRow(StringData.fromString("id10"), StringData.fromString("Ella"), 33L,
                  TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
        insertRow(StringData.fromString("id11"), StringData.fromString("Phoebe"), 44L,
                  TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
    );
    writeData(dataSetInsert, conf);

    InputFormat<RowData, ?> inputFormat;
    List<RowData> result;
    String actual;
    String expected;

    // write another commit to read again
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH, Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    set_row_type(ROW_DATA_TYPE);
    List<RowData> dataSetInsert1 = Arrays.asList(
        // new data
        insertRow(StringData.fromString("id99"), StringData.fromString("Jane"), 99,
                  TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
        insertRow(StringData.fromString("id100"), StringData.fromString("Ella"), 100,
                  TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
        insertRow(StringData.fromString("id111"), StringData.fromString("Phoebe"), 111,
                  TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
    );
    writeData(dataSetInsert1, conf);

    this.tableSource.reset();

    ResolvedSchema newSchema = SchemaBuilder.instance()
        .fields(ROW_TYPE.getFieldNames(), ROW_DATA_TYPE.getChildren())
        .build();
    this.tableSource = new HoodieTableSource(
            newSchema,
        new Path(tempFile.getAbsolutePath()),
        Collections.singletonList("partition"),
        "default",
        conf);
    inputFormat = this.tableSource.getInputFormat();

    result = readData(inputFormat);

    actual = TestData.rowDataToString(result, ROW_DATA_TYPE);
    expected = "["
        + "+I[id9, Jane, 22, 1970-01-01T00:00:00.006, par3], "
        + "+I[id10, Ella, 33, 1970-01-01T00:00:00.007, par4], "
        + "+I[id11, Phoebe, 44, 1970-01-01T00:00:00.008, par4],"
        + "+I[id99, Jane, 99, 1970-01-01T00:00:00.006, par3], "
        + "+I[id100, Ella, 100, 1970-01-01T00:00:00.007, par4], "
        + "+I[id111, Phoebe, 111, 1970-01-01T00:00:00.008, par4]]";
    assertThat(actual, is(expected));
  }

  @Test
  void testReadBaseAndLogFiles() throws Exception {
    beforeEach(HoodieTableType.MERGE_ON_READ);

    // write base first with compaction
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    InputFormat<RowData, ?> inputFormat = this.tableSource.getInputFormat();

    List<RowData> result = readData(inputFormat);

    String actual = TestData.rowDataToString(result);
    String expected = TestData.rowDataToString(TestData.DATA_SET_INSERT);
    assertThat(actual, is(expected));

    // write another commit using logs and read again
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    // write another commit using logs with separate partition
    // so the file group has only logs
    TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);

    // refresh the input format
    this.tableSource.reset();
    inputFormat = this.tableSource.getInputFormat();

    result = readData(inputFormat);

    actual = TestData.rowDataToString(result);
    expected = "["
        + "+I[id1, Danny, 24, 1970-01-01T00:00:00.001, par1], "
        + "+I[id2, Stephen, 34, 1970-01-01T00:00:00.002, par1], "
        + "+I[id3, Julian, 54, 1970-01-01T00:00:00.003, par2], "
        + "+I[id4, Fabian, 32, 1970-01-01T00:00:00.004, par2], "
        + "+I[id5, Sophia, 18, 1970-01-01T00:00:00.005, par3], "
        + "+I[id6, Emma, 20, 1970-01-01T00:00:00.006, par3], "
        + "+I[id7, Bob, 44, 1970-01-01T00:00:00.007, par4], "
        + "+I[id8, Han, 56, 1970-01-01T00:00:00.008, par4], "
        + "+I[id9, Jane, 19, 1970-01-01T00:00:00.006, par3], "
        + "+I[id10, Ella, 38, 1970-01-01T00:00:00.007, par4], "
        + "+I[id11, Phoebe, 52, 1970-01-01T00:00:00.008, par4], "
        + "+I[id12, Monica, 27, 1970-01-01T00:00:00.009, par5], "
        + "+I[id13, Phoebe, 31, 1970-01-01T00:00:00.010, par5], "
        + "+I[id14, Rachel, 52, 1970-01-01T00:00:00.011, par6], "
        + "+I[id15, Ross, 29, 1970-01-01T00:00:00.012, par6]]";
    assertThat(actual, is(expected));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testReadIncrementally(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_INCREMENTAL);
    beforeEach(tableType, options);

    // write another commit to read again
    for (int i = 0; i < 6; i += 2) {
      List<RowData> dataset = TestData.dataSetInsert(i + 1, i + 2);
      TestData.writeData(dataset, conf);
    }

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(conf));
    List<String> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());

    assertThat(commits.size(), is(3));

    // only the start commit
    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat1 = this.tableSource.getInputFormat();
    assertThat(inputFormat1, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual1 = readData(inputFormat1);
    final List<RowData> expected1 = TestData.dataSetInsert(3, 4, 5, 6);
    TestData.assertRowDataEquals(actual1, expected1);

    // only the start commit: earliest
    conf.setString(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat2 = this.tableSource.getInputFormat();
    assertThat(inputFormat2, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual2 = readData(inputFormat2);
    final List<RowData> expected2 = TestData.dataSetInsert(1, 2, 3, 4, 5, 6);
    TestData.assertRowDataEquals(actual2, expected2);

    // start and end commit: [start commit, end commit]
    conf.setString(FlinkOptions.READ_START_COMMIT, commits.get(0));
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat3 = this.tableSource.getInputFormat();
    assertThat(inputFormat3, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual3 = readData(inputFormat3);
    final List<RowData> expected3 = TestData.dataSetInsert(1, 2, 3, 4);
    TestData.assertRowDataEquals(actual3, expected3);

    // only the end commit: point in time query
    conf.removeConfig(FlinkOptions.READ_START_COMMIT);
    conf.setString(FlinkOptions.READ_END_COMMIT, commits.get(1));
    this.tableSource = getTableSource(conf);
    InputFormat<RowData, ?> inputFormat4 = this.tableSource.getInputFormat();
    assertThat(inputFormat4, instanceOf(MergeOnReadInputFormat.class));

    List<RowData> actual4 = readData(inputFormat4);
    final List<RowData> expected4 = TestData.dataSetInsert(3, 4);
    TestData.assertRowDataEquals(actual4, expected4);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private HoodieTableSource getTableSource(Configuration conf) {
    return new HoodieTableSource(
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getAbsolutePath()),
        Collections.singletonList("partition"),
        "default",
        conf);
  }

  @SuppressWarnings("unchecked, rawtypes")
  private static List<RowData> readData(InputFormat inputFormat) throws IOException {
    InputSplit[] inputSplits = inputFormat.createInputSplits(1);

    List<RowData> result = new ArrayList<>();

    for (InputSplit inputSplit : inputSplits) {
      inputFormat.open(inputSplit);
      while (!inputFormat.reachedEnd()) {
        result.add(TestConfigurations.SERIALIZER.copy((RowData) inputFormat.nextRecord(null))); // no reuse
      }
      inputFormat.close();
    }
    return result;
  }

  private static void writeData(
          List<RowData> dataBuffer,
          Configuration conf) throws Exception {
    StreamWriteFunctionWrapper<RowData> funcWrapper = new StreamWriteFunctionWrapper<>(
            conf.getString(FlinkOptions.PATH),
            conf);
    funcWrapper.openFunction();

    for (RowData rowData : dataBuffer) {
      funcWrapper.invoke(rowData);
    }

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    funcWrapper.checkpointComplete(1);

    funcWrapper.close();
  }
}
