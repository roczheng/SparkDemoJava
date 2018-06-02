package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/*********************************
 * @Created on 2018-6-2.   ******
 * @Author: _photoAndCoding_ ******
 * @Version: 1.0          ******
 ********************************/
public class SparkReadOracleDemo {

    public static void main(String[] args) {
        //Windows下建运行项目的时候会加载hadoop的内容，这里配置了个windows版的
        System.setProperty("hadoop.home.dir", "D:\\hadoopbin\\");
        String master = "local";
        String appName = "SparkJdbcDemo";
        SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .config("spark.driver.cores","1")
                .getOrCreate();
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@10.124.0.41:1521:ngact")
                .option("dbtable", "tf_f_user")
                .option("user", "UOP_ACT1")
                .option("password", "cbss_UOP_ACT1")
                .load();
        jdbcDF.show();
        jdbcDF.createOrReplaceTempView("TEST_TABLE");
        Dataset<Row> result = spark.sql("SELECT * FROM TEST_TABLE");
        result.show();
    }
}
