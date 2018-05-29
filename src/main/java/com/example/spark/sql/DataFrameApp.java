package com.example.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DataFrames
 *
 * bash:
 * bin/spark-submit --class "com.example.spark.sql.DataFrameApp" spark-example-1.0-SNAPSHOT.jar
 *
 * @author xuan
 * @since 1.0.0
 */
public class DataFrameApp {

    public static void main(String[] args) {
        // The entry point into all functionality in Spark is the SparkSession class
        SparkSession spark = SparkSession
                .builder()
                .appName(DataFrameApp.class.getSimpleName())
                .getOrCreate();

        // creates a DataFrame based on the content of a JSON file
        Dataset<Row> df = spark.read().json("file:///usr/local/spark/people.json");

        // Displays the content of the DataFrame to stdout
        df.show();

        // Select only the "name" column
        df.select("name").show();

        // Select everybody, but increment the age by 1
        df.select(df.col("name"), df.col("age").plus(1)).show();

        // Select people older than 21
        df.filter(df.col("age").gt(21)).show();

        // Save
        df.javaRDD().saveAsTextFile("file:///usr/local/spark/people.txt");

        // Creates a temporary view using the DataFrame
        df.createOrReplaceTempView("people");
        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results  = spark.sql("select name from people");
        // Displays the content of the DataFrame to stdout
        results.show();

        // close
        spark.close();
    }

}
