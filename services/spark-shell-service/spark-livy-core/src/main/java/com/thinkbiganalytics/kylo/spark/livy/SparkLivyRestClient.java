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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.thinkbiganalytics.kylo.exceptions.LivyException;
import com.thinkbiganalytics.kylo.model.Session;
import com.thinkbiganalytics.kylo.model.SessionsGetResponse;
import com.thinkbiganalytics.kylo.model.Statement;
import com.thinkbiganalytics.kylo.model.StatementsPost;
import com.thinkbiganalytics.kylo.model.enums.SessionState;
import com.thinkbiganalytics.kylo.model.enums.StatementState;
import com.thinkbiganalytics.kylo.utils.LivyRestModelTransformer;
import com.thinkbiganalytics.kylo.utils.ScalaScriptService;
import com.thinkbiganalytics.kylo.utils.ScriptGenerator;
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
import java.util.Objects;
import java.util.Optional;

public class SparkLivyRestClient implements SparkShellRestClient {
    private static final Logger logger = LoggerFactory.getLogger(SparkLivyRestClient.class);

    @Resource
    private SparkLivyProcessManager sparkLivyProcessManager;

    @Resource
    private ScriptGenerator scriptGenerator;

    @Resource
    private ScalaScriptService scalaScriptService;

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
        logger.info("getTransformResult(process,table) called");

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        String script = scriptGenerator.script("getDataSources");

        StatementsPost sp = new StatementsPost.Builder().code(script).kind("spark").build();

        Integer sessionId = sparkLivyProcessManager.getLivySessionId(process);
        Statement statement = client.post(String.format("/sessions/%s/statements", sessionId ),
                sp,
                Statement.class);
        logger.info("{}", statement);

        if( statement.getState() == StatementState.running
                || statement.getState() == StatementState.waiting) {
            statement = getStatement( client, sessionId, statement.getId() );
            logger.info("{}", statement);
        } else {
            throw new LivyException("Unexpected error");
        }

        return LivyRestModelTransformer.toDataSources(statement);
    }


    // TODO: is there a better way to wait for response than synchronous?  UI could poll?
    private Statement getStatement(JerseyRestClient jerseyClient, Integer sessionId, Integer stmtId) {
        Statement statement = null;
        do {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            statement = jerseyClient.get(String.format("/sessions/%s/statements/%s", sessionId, stmtId),
                    Statement.class);
            logger.debug("getStatement statement={}", statement);
        } while (statement == null || ! statement.getState().equals(StatementState.available));

        return statement;
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

        Statement statement;
        try {
            Integer stmtId = Integer.parseInt(saveId);
            statement = client.get(
                    String.format("/sessions/%s/statements/%s", sparkLivyProcessManager.getLivySessionId(process), stmtId),
                    Statement.class);
            sparkLivyProcessManager.setLastStatementId(process, statement.getId());
            logger.info("getStatement id={}, spr={}", stmtId, statement);
        } catch (NumberFormatException nfe) {
            // saveId is not an integer.  We have already processed Livy Response
            String script = scriptGenerator.script("getSave", saveId);

            StatementsPost sp = new StatementsPost.Builder().code(script).kind("spark").build();
            statement = client.post(String.format("/sessions/%s/statements", sparkLivyProcessManager.getLivySessionId(process)),
                    sp,
                    Statement.class);

            logger.info("{}", statement);
        }

        SaveResponse response = LivyRestModelTransformer.toSaveResponse(statement);
        return Optional.of(response);
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
        logger.info("saveTransform(process,id,saveRequest) called");

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        String format = scalaStr(request.getFormat());
        String mode = scalaStr(request.getMode());
        String tableName = scalaStr(request.getTableName());
        String makeRequest = scriptGenerator.wrappedScript("newSaveRequest", "", "\n", format,
                mode, tableName);
        String script = scriptGenerator.wrappedScript("submitSaveJob", makeRequest, "\n", transformId);

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


    private static String scalaStr( Object o ) {
        if( o == null ) {
            return "null";
        } else {
            return '"' + String.valueOf(o) + '"';
        }
    }

    @Nonnull
    @Override
    public TransformResponse transform(@Nonnull SparkShellProcess process, @Nonnull TransformRequest request) {
        logger.info("transform(process,request) called");

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        // a tablename will be calculated for the request
        String script = scalaScriptService.wrapScriptForLivy(request);

        StatementsPost sp = new StatementsPost.Builder().code(script).kind("spark").build();

        Statement statement = client.post(String.format("/sessions/%s/statements", sparkLivyProcessManager.getLivySessionId(process)),
                sp,
                Statement.class);

        logger.info("statement={}", statement);
        sparkLivyProcessManager.setLastStatementId(process, statement.getId());

        TransformResponse response = new TransformResponse();
        response.setStatus(StatementStateTranslator.translate(statement.getState()));
        response.setProgress(statement.getProgress());

        response.setTable(scalaScriptService.transformCache.getIfPresent(request));

        return response;
    }

}
