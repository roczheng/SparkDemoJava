package com.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/*********************************
 * @Created on 2018-6-2.   ******
 * @Author: _photoAndCoding ******
 * @Version: 1.0          ******
 ********************************/
public class SparkReadMysqlDemo {

    public static void main(String[] args){
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

        //创建配置文件
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbtable", "TEST_TABLE");
        connectionProperties.setProperty("user", "username");
        connectionProperties.setProperty("password","password");
        connectionProperties.setProperty("driver","com.mysql.jdbc.Driver");

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> drdsDF = sqlContext.read().jdbc("jdbc:mysql://x.x.x.x:3306/dbname", "TEST_TABLE", connectionProperties);
        drdsDF.show();

    }
}
