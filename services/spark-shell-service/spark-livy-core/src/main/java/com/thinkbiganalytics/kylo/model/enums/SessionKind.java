package com.thinkbiganalytics.kylo.model.enums;

/**
 * Session Kind
 * Value	Description
 * spark	Interactive Scala Spark session
 * pyspark	Interactive Python Spark session
 * sparkr	Interactive R Spark session
 * sql	Interactive SQL Spark session
 */
public enum SessionKind {
    spark,
    pyspark,
    sparkr,
    sql
}
