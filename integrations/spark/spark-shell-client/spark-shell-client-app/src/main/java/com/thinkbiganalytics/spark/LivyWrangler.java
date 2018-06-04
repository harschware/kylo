package com.thinkbiganalytics.spark;

/*-
 * #%L
 * kylo-spark-shell-client-app
 * %%
 * Copyright (C) 2017 - 2018 ThinkBig Analytics, a Teradata Company
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.thinkbiganalytics.spark.conf.LivyWranglerConfig;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 */
public class LivyWrangler {

    /**
     * Main entry point into program
     *
     * @param args: list of args
     */
    public static void main(String[] args) {
    }


    public static ApplicationContext createSpringContext(SparkContext sc, SQLContext sqlContext) {
        LivyWranglerConfig.setSparkContext(sc);
        LivyWranglerConfig.setSqlContext(sqlContext);
        ApplicationContext context = new AnnotationConfigApplicationContext(new Class[]{LivyWranglerConfig.class});

        return context;
    }


    /*
    import scala.collection.mutable._
    case class TestPerson(name: String, age: Long, salary: Double)
    val tom = TestPerson("Tom Hanks",37,35.5)
    val sam = TestPerson("Sam Smith",40,40.5)
    val PersonList = MutableList[TestPerson]()
    PersonList += tom
    PersonList += sam
    val personDF = PersonList.toDF()
    val namesDf = personDF.select("name")

    import harschware.sandbox.drivers.LivyWrangler
    import com.thinkbiganalytics.spark.dataprofiler.Profiler
    val ctx = LivyWrangler.createSpringContext(sc, sqlContext)
    val profiler = ctx.getBean(classOf[Profiler])
    LivyWrangler.profileDataFrame(profiler,namesDf)

    ctx.getBeanDefinitionNames().foreach(println)

    // SINGLE LINE TEST
    val ctx = harschware.sandbox.drivers.LivyWrangler.createSpringContext(sc, sqlContext)
     */
    /*
    public static List<OutputRow> profileDataFrame(com.thinkbiganalytics.spark.dataprofiler.Profiler profiler, DataFrame dataFrame) {
        DataSet16 dataSet16 = new DataSet16(dataFrame);

        // Profile data set
        ProfilerConfiguration profilerConfiguration = new ProfilerConfiguration();
        profilerConfiguration.setNumberOfTopNValues(50);
        profilerConfiguration.setBins(35);
        final StatisticsModel dataStats = profiler.profile(dataSet16, profilerConfiguration);

        // Add stats to result
        if (dataStats != null) {
            List<OutputRow> profile = new ArrayList<OutputRow>(dataStats.getColumnStatisticsMap().size());

            for (final ColumnStatistics columnStats : dataStats.getColumnStatisticsMap().values()) {
                profile.addAll(columnStats.getStatistics());
            }
            return profile;
        }

        // no stats
        return Lists.newArrayList();
    }
    */

} // end class
