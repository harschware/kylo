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

import com.google.common.collect.Lists;
import com.thinkbiganalytics.kylo.model.Statement;
import com.thinkbiganalytics.kylo.model.StatementsPost;
import com.thinkbiganalytics.kylo.utils.LivyRestModelTransformer;
import com.thinkbiganalytics.kylo.utils.ScalaScripUtils;
import com.thinkbiganalytics.kylo.utils.StatementStateTranslator;
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
        DataSources ds = new DataSources();
        ds.setDownloads(Lists.newArrayList());
        ds.setTables(Lists.newArrayList("orc"));
        return ds;
        // TODO: actually calculate datasources
        //throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<TransformResponse> getQueryResult(@Nonnull SparkShellProcess process, @Nonnull String id) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<TransformResponse> getTransformResult(@Nonnull SparkShellProcess process, @Nonnull String transformId) {
        logger.info("getTransformResult(process,table) called");

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        Integer stmtId = sparkLivyProcessManager.getLastStatementId(process);

        Statement statement = client.get(String.format("/sessions/%s/statements/%s", sparkLivyProcessManager.getLivySessionId(process), stmtId),
                Statement.class);

        sparkLivyProcessManager.setLastStatementId(process, statement.getId());
        logger.info("getStatement id={}, spr={}", stmtId, statement);
        TransformResponse response = LivyRestModelTransformer.toTransformResponse(statement, transformId);
        return Optional.of(response);
    }

    @Nonnull
    @Override
    public Optional<SaveResponse> getQuerySave(@Nonnull SparkShellProcess process, @Nonnull String queryId, @Nonnull String saveId) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<SaveResponse> getTransformSave(@Nonnull SparkShellProcess process, @Nonnull String transformId, @Nonnull String saveId) {
        logger.info("getTransformResult(process,table) called");

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        try {
            Integer stmtId = Integer.parseInt(saveId);
            Statement statement = client.get(String.format("/sessions/%s/statements/%s", sparkLivyProcessManager.getLivySessionId(process), stmtId),
                    Statement.class);
            sparkLivyProcessManager.setLastStatementId(process, statement.getId());
            logger.info("getStatement id={}, spr={}", stmtId, statement);
            SaveResponse response = LivyRestModelTransformer.toSaveResponse(statement, transformId);
            return Optional.of(response);
        } catch( NumberFormatException nfe ) {
            // saveId is not an integer.  We have already processed Livy Response
            String script = "val response = sparkShellTransformController.getSave(\"" + saveId + "\");\n" +
                    "val saveResponse =  response.getEntity().asInstanceOf[com.thinkbiganalytics.spark.rest.model.SaveResponse]\n" +
                    "val sr = mapper.writeValueAsString(saveResponse)\n" +
                    "%json sr\n";

            StatementsPost sp = new StatementsPost.Builder().code(script).kind("spark").build();

            Statement statement = client.post(String.format("/sessions/%s/statements", sparkLivyProcessManager.getLivySessionId(process)),
                    sp,
                    Statement.class);

            logger.info("{}", statement);

            SaveResponse response = new SaveResponse();
            response.setId(statement.getId().toString());
            response.setStatus(StatementStateTranslator.translateToSaveResponse(statement.getState()));
            response.setProgress(statement.getProgress());
            return Optional.of(response);
        }
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
    public SaveResponse saveTransform(@Nonnull SparkShellProcess process, @Nonnull String transformId, @Nonnull SaveRequest request) {
        // TODO: fix me
        logger.info("saveTransform(process,id,saveRequest) called");

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        String makeRequest = "val sr: com.thinkbiganalytics.spark.rest.model.SaveRequest = new com.thinkbiganalytics.spark.rest.model.SaveRequest()\n" +
                "sr.setFormat(\"" + request.getFormat() + "\")\n" +
                "sr.setJdbc(null)\n" +
                "sr.setMode(\"" + request.getMode() + "\")\n" +
                "sr.setOptions(new java.util.HashMap[String,String])\n" +
                "sr.setTableName(\"" + request.getTableName() + "\")\n";
        String script = makeRequest +
                "val t" + transformId + "= sqlContext.sql(\"select * from " + transformId + "\")\n" +
                "val dataSet = sparkContextService.toDataSet(t" + transformId + ")\n" +
                "val saveResponse = transformService.submitSaveJob(transformService.createSaveTask(sr, new com.thinkbiganalytics.spark.metadata.ShellTransformStage(dataSet, converterService)))\n" +
                "val sr = mapper.writeValueAsString(saveResponse)\n" +
                "%json sr";

        StatementsPost sp = new StatementsPost.Builder().code(script).kind("spark").build();

        Statement statement = client.post(String.format("/sessions/%s/statements", sparkLivyProcessManager.getLivySessionId(process)),
                sp,
                Statement.class);

        logger.info("{}", statement);

        SaveResponse saveResponse = new SaveResponse();
        saveResponse.setId(statement.getId().toString());
        saveResponse.setStatus(SaveResponse.Status.LIVY_PENDING);
        saveResponse.setProgress(statement.getProgress());

        return saveResponse;
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

        // a tablename will be calculated for the request
        String script = ScalaScripUtils.wrapScriptForLivy(request);

        StatementsPost sp = new StatementsPost.Builder().code(script).kind("spark").build();

        Statement statement = client.post(String.format("/sessions/%s/statements", sparkLivyProcessManager.getLivySessionId(process)),
                sp,
                Statement.class);

        logger.info("statement={}", statement);
        sparkLivyProcessManager.setLastStatementId(process, statement.getId());

        TransformResponse response = new TransformResponse();
        response.setStatus(StatementStateTranslator.translate(statement.getState()));
        response.setProgress(statement.getProgress());

        response.setTable(ScalaScripUtils.transformCache.getIfPresent(request));

        return response;
    }


}

