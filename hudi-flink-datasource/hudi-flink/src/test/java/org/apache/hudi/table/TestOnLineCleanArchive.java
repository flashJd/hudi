package org.apache.hudi.table;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.junit.Ignore;

import java.io.File;
import java.util.concurrent.ExecutionException;

import static org.apache.hudi.utils.TestConfigurations.FIELDS01;
import static org.apache.hudi.utils.TestConfigurations.sql;

@Ignore
public class TestOnLineCleanArchive {
  static TableEnvironment streamTableEnv;
  static File tempFile = new File("./hudi_table_test");

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

    FileUtils.deleteDirectory(tempFile);
    tempFile.mkdirs();

    // Configuration conf = new Configuration();
    // StreamExecutionEnvironment env =
    // StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    // streamTableEnv = StreamTableEnvironment.create(env);

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
            + "  'rows-per-second'='1',\n"
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

    String hoodieTableDDL =
        sql("t1")
            .fields(FIELDS01)
            .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
            .option("table.type", "COPY_ON_WRITE")
            .option("write.bucket_assign.tasks", "4")
            .option("hoodie.clean.automatic", "true")
            .option("clean.async.enabled", "true")
            .option("compaction.async.enabled", "true")
            .option("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS")
            .option("hoodie.cleaner.commits.retained", "1")
            .option("hoodie.cleaner.policy.failed.writes", "LAZY")
            .option("hoodie.clean.allow.multiple", "false")
            .option("hoodie.cleaner.delete.bootstap.base.file", "false")
            .option("hoodie.archive.automatic", "true")
            .option("hoodie.archive.async", "false")
            .option("hoodie.keep.max.commits", "5")
            .option("hoodie.keep.min.commits", "3")
            .option("hoodie.archive.beyond.savepoint", "true")
            .end();
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from datagen";
    execInsertSql(streamTableEnv, insertInto);

    //    List<Row> rows = CollectionUtil.iterableToList(
    //        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    //    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }
}
