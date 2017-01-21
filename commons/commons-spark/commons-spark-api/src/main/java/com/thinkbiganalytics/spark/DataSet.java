package com.thinkbiganalytics.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * A collection of objects that can be transformed using Spark functions.
 */
public interface DataSet {

    /**
     * Returns the content of this data set as a Spark RDD.
     * @return a Spark RDD
     */
    JavaRDD<Row> javaRDD();

    /**
     * Filters rows using the specified SQL expression.
     * @param condition a SQL expression
     * @return the filtered data set
     */
    DataSet filter(String condition);

    /**
     * Drops the specified column from this data set.
     * @param condition the column to be dropped
     * @return the data set without the column
     */
    DataSet drop(String condition);

    /**
     * Converts this strongly-typed data set to a generic data set.
     * @return the generic data set
     */
    DataSet toDF();

    /**
     * Returns the number of rows in this data set.
     * @return the row count
     */
    long count();

    /**
     * Registers this data set as a temporary table with the specified name.
     * @param tableName the name for the temporary table
     */
    void registerTempTable(String tableName);

    /**
     * Returns the schema of this data set.
     * @return the schema
     */
    StructType schema();

    /**
     * Returns a list that contains all rows in this data set.
     * @return the rows
     */
    List<Row> collectAsList();

    /**
     * Saves the content of this data set as the specified table.
     * @param partitionColumn the name of the partition column
     * @param fqnTable the name for the table
     */
    void writeToTable(String partitionColumn, String fqnTable);
}
