package com.dpf.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchSql {

    public static String hiveCatalog = "hive";
    public static String defaultDatabase = "default";
    public static String hiveConfDir = "/Users/dengpengfei/bigdata/apache-hive-3.1.2-bin/conf";

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //添加hive catalog
        HiveCatalog hive = new HiveCatalog(hiveCatalog, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(hiveCatalog, hive);

        tableEnv.useCatalog(hiveCatalog);

        tableEnv.executeSql("select * from hive_table").print();
    }
}
