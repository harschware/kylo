package com.thinkbiganalytics.kylo.spark.livy;

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

import com.thinkbiganalytics.spark.shell.SparkShellProcess;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class SparkLivyProcess implements SparkShellProcess {
    private static final Logger logger = LoggerFactory.getLogger(SparkLivyProcess.class);

    private UUID id;

    public SparkLivyProcess(UUID id) {
        this.id = id;
    }

    @Nonnull
    @Override
    public String getClientId() {
        return id.toString();
    }

    @Nonnull
    @Override
    public String getHostname() {
        return "sandbox.kylo.io";
    }

    @Override
    public int getPort() {
        return 8998;
    }

    @Override
    public boolean isLocal() {
        return true;
    }


    public static SparkLivyProcess newInstance() {
        return new SparkLivyProcess(UUID.randomUUID());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SparkLivyProcess that = (SparkLivyProcess) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SparkLivyProcess{");
        sb.append("id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}
