package com.thinkbiganalytics.spark.dataprofiler.config;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

@Configuration
@ComponentScan(basePackages = {"com.thinkbiganalytics.spark"}, excludeFilters={
        @ComponentScan.Filter(type=FilterType.ASSIGNABLE_TYPE, value=SparkContext.class),
        @ComponentScan.Filter(type=FilterType.ASSIGNABLE_TYPE, value=SQLContext.class),
})
public class LivyProfilerConfig {
    private static SparkContext sparkContext;
    private static SQLContext sqlContext;

    public static void setSparkContext(SparkContext sparkContext) {
        LivyProfilerConfig.sparkContext = sparkContext;
    }

    public static void setSqlContext(SQLContext sqlContext) {
        LivyProfilerConfig.sqlContext = sqlContext;
    }

    @Bean
    public SparkContext sparkContext() {
        return sparkContext;
    }

    @Bean
    public SQLContext sqlContext() {
        return sqlContext;
    }

}
