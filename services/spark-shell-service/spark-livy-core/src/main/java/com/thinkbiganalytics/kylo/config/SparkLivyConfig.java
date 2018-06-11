package com.thinkbiganalytics.kylo.config;

/*-
 * #%L
 * kylo-spark-livy-core
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

import com.thinkbiganalytics.kylo.spark.conf.KerberosSparkProperties;
import com.thinkbiganalytics.kylo.spark.livy.SparkLivyProcessManager;
import com.thinkbiganalytics.kylo.spark.livy.SparkLivyRestClient;
import com.thinkbiganalytics.kylo.utils.ScriptGenerator;
import com.thinkbiganalytics.spark.shell.SparkShellRestClient;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;

import java.io.IOException;

@Configuration
@PropertySource("classpath:spark.properties")
@ImportResource("classpath:/config/applicationContext-livy.xml")
public class SparkLivyConfig {

    /**
     * Loads the properties for acquiring a Kerberos ticket.
     *
     * @return the Kerberos properties
     */
    @Bean
    @ConfigurationProperties("kerberos.spark")
    public KerberosSparkProperties kerberosSparkProperties() {
        return new KerberosSparkProperties();
    }

    /**
     * @return a Spark Livy client
     * @implNote required to start Kylo
     */
    @Bean
    public SparkShellRestClient restClient() {
        return new SparkLivyRestClient();
    }

    /**
     * @return
     * @implNote required to start Kylo
     */
    @Bean
    public SparkLivyProcessManager sparkShellProcessManager() {
        return new SparkLivyProcessManager();
    }

    @Bean
    public ScriptGenerator scriptGenerator() {
        return new ScriptGenerator();
    }


    /**
     * Gets the Hadoop File System.
     */
    @Bean
    public FileSystem fileSystem() throws IOException {
        return FileSystem.get(new org.apache.hadoop.conf.Configuration());
    }

}
