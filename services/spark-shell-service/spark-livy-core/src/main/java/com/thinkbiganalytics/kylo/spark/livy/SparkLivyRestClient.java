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

import com.thinkbiganalytics.kylo.model.StatementsPost;
import com.thinkbiganalytics.kylo.model.StatementsPostResponse;
import com.thinkbiganalytics.kylo.utils.ScalaScripUtils;
import com.thinkbiganalytics.rest.JerseyRestClient;
import com.thinkbiganalytics.spark.rest.model.*;
import com.thinkbiganalytics.spark.shell.SparkShellProcess;
import com.thinkbiganalytics.spark.shell.SparkShellRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Resource;
import javax.ws.rs.core.Response;
import java.util.Optional;

public class SparkLivyRestClient implements SparkShellRestClient {
    private static final Logger logger = LoggerFactory.getLogger(SparkLivyRestClient.class);


    @Resource
    private SparkLivyProcessManager sparkLivyProcessManager;

    @Nonnull
    @Override
    public Optional<Response> downloadQuery(@Nonnull SparkShellProcess process, @Nonnull String queryId, @Nonnull String saveId) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<Response> downloadTransform(@Nonnull SparkShellProcess process, @Nonnull String transformId, @Nonnull String saveId) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public DataSources getDataSources(@Nonnull SparkShellProcess process) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<TransformResponse> getQueryResult(@Nonnull SparkShellProcess process, @Nonnull String id) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<TransformResponse> getTransformResult(@Nonnull SparkShellProcess process, @Nonnull String table) {
        // TODO:   query for results
        logger.info("getTransformResult(process,table) called");

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        StatementsPostResponse result = client.get(String.format("/sessions/%s/statements/0", sparkLivyProcessManager.getLivySessionId(process)),
                StatementsPostResponse.class);
        logger.info("getStatement id={}, result={}", 0, result);

        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<SaveResponse> getQuerySave(@Nonnull SparkShellProcess process, @Nonnull String queryId, @Nonnull String saveId) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<SaveResponse> getTransformSave(@Nonnull SparkShellProcess process, @Nonnull String transformId, @Nonnull String saveId) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public TransformResponse query(@Nonnull SparkShellProcess process, @Nonnull TransformRequest request) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public SaveResponse saveQuery(@Nonnull SparkShellProcess process, @Nonnull String id, @Nonnull SaveRequest request) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public SaveResponse saveTransform(@Nonnull SparkShellProcess process, @Nonnull String id, @Nonnull SaveRequest request) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public TransformResponse transform(@Nonnull SparkShellProcess process, @Nonnull TransformRequest request) {
        /*try {
            return getClient(process).post(TRANSFORM_PATH, request, TransformResponse.class);
        } catch (final InternalServerErrorException e) {
            throw propagateTransform(e);
        } */

        logger.info("transform(process,request) called");

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);


        String script = ScalaScripUtils.dataFrameToJava( request.getScript() );

        StatementsPost sp = new StatementsPost.Builder().code(script).kind("spark").build();

        String s = client.post(String.format("/sessions/%s/statements", sparkLivyProcessManager.getLivySessionId(process)),
                sp,
                String.class);

        logger.info("getStatements={}", s);

        TransformResponse response = new TransformResponse();
        response.setStatus(TransformResponse.Status.PENDING);
        response.setProgress(0.0);

        return response;
        //throw new UnsupportedOperationException();
    }


}
