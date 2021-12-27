package com.dpf.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Skeleton for a Flink Streaming SQL.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingSql {

    public static String hiveCatalog = "hive";
    public static String defaultCatalog = "default_catalog";
    public static String defaultDatabase = "default";
    public static String hiveConfDir = "/Users/dengpengfei/bigdata/apache-hive-3.1.2-bin/conf";

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT,"8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.disableOperatorChaining();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);

        HiveCatalog hive = new HiveCatalog(hiveCatalog, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(hiveCatalog, hive);

        String hiveTableSql = "" +
                "CREATE TABLE IF NOT EXISTS hive_table (" +
                "  user_id STRING," +
                "  order_amount BIGINT" +
                ") PARTITIONED BY (dt STRING, hour STRING, min STRING) STORED AS parquet " +
                "TBLPROPERTIES (" +
                "  'is_generic'='false'," +
                "  'partition.time-extractor.timestamp-pattern'='$dt $hour:$min:00'," +
                "  'sink.partition-commit.trigger'='partition-time'," +
                "  'sink.partition-commit.delay'='1 min'," +
                "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai'," +
                "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                ")";

        String kafkaTableSql = "" +
                " CREATE TABLE IF NOT EXISTS kafka_table ( " +
                "  user_id STRING," +
                "  order_amount BIGINT," +
                "  ts TIMESTAMP(3) METADATA FROM 'timestamp'," +
                "  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND" +
                " ) WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = 'source_order', " +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'properties.zookeeper.connect' = '127.0.0.1:2181', " +
                "  'properties.bootstrap.servers' = '127.0.0.1:9092', " +
                "  'properties.group.id' = 'testGroup'," +
                "  'format' = 'json'," +
                "  'json.fail-on-missing-field' = 'false'," +
                "  'json.ignore-parse-errors' = 'true'" +
                " )";

        String etlSql = "" +
                "INSERT INTO `hive`.`default`.`hive_table` " +
                "SELECT user_id, order_amount, DATE_FORMAT(`ts`, 'yyyy-MM-dd'), DATE_FORMAT(`ts`, 'HH'), CONCAT(LPAD(DATE_FORMAT(`ts`, 'mm'), 1, '??'), '0') FROM kafka_table";

        // to user hive catalog
        tableEnv.useCatalog(hiveCatalog);
        // to use hive dialect
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql(hiveTableSql);

        // to user default catalog
        tableEnv.useCatalog(defaultCatalog);
        // to use default dialect
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql(kafkaTableSql);

        tableEnv.executeSql(etlSql);
    }
}
