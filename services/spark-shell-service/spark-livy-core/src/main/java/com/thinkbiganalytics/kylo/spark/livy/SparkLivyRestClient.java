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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkbiganalytics.kylo.spark.SparkException;
import com.thinkbiganalytics.kylo.spark.client.LivyClient;
import com.thinkbiganalytics.kylo.spark.client.model.LivyServer;
import com.thinkbiganalytics.kylo.spark.config.LivyProperties;
import com.thinkbiganalytics.kylo.spark.exceptions.LivyException;
import com.thinkbiganalytics.kylo.spark.model.Statement;
import com.thinkbiganalytics.kylo.spark.model.StatementsPost;
import com.thinkbiganalytics.kylo.spark.model.enums.StatementState;
import com.thinkbiganalytics.kylo.spark.rest.model.job.SparkJobRequest;
import com.thinkbiganalytics.kylo.spark.rest.model.job.SparkJobResponse;
import com.thinkbiganalytics.kylo.utils.LivyRestModelTransformer;
import com.thinkbiganalytics.kylo.utils.ScalaScriptService;
import com.thinkbiganalytics.kylo.utils.ScalaScriptUtils;
import com.thinkbiganalytics.kylo.utils.ScriptGenerator;
import com.thinkbiganalytics.kylo.utils.StatementStateTranslator;
import com.thinkbiganalytics.rest.JerseyRestClient;
import com.thinkbiganalytics.spark.io.ZipStreamingOutput;
import com.thinkbiganalytics.spark.model.SaveResult;
import com.thinkbiganalytics.spark.rest.model.DataSources;
import com.thinkbiganalytics.spark.rest.model.KyloCatalogReadRequest;
import com.thinkbiganalytics.spark.rest.model.SaveRequest;
import com.thinkbiganalytics.spark.rest.model.SaveResponse;
import com.thinkbiganalytics.spark.rest.model.ServerStatusResponse;
import com.thinkbiganalytics.spark.rest.model.SimpleResponse;
import com.thinkbiganalytics.spark.rest.model.TransformRequest;
import com.thinkbiganalytics.spark.rest.model.TransformResponse;
import com.thinkbiganalytics.spark.shell.SparkShellProcess;
import com.thinkbiganalytics.spark.shell.SparkShellRestClient;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Resource;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class SparkLivyRestClient implements SparkShellRestClient {

    private static final XLogger logger = XLoggerFactory.getXLogger(SparkLivyRestClient.class);

    @Resource
    private SparkLivyProcessManager sparkLivyProcessManager;

    @Resource
    private ScriptGenerator scriptGenerator;

    @Resource
    private ScalaScriptService scalaScriptService;

    @Resource
    private LivyClient livyClient;

    @Resource
    private LivyServer livyServer;

    @Resource
    private LivyProperties livyProperties;


    /**
     * Default file system
     */
    @Resource
    public FileSystem fileSystem;

    /**
     * Cache of transformId => saveid
     */
    @Nonnull
    final static Cache<String, Integer> transformIdsToLivyId = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS)
        .maximumSize(100)
        .build();

    /**
     * Cache of transformId => saveid
     */
    @Nonnull
    final public static Cache<Integer, String> livyIdToTransformId = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS)
        .maximumSize(100)
        .build();

    @Override
    public SparkJobResponse createJob(@Nonnull final SparkShellProcess process, @Nonnull final SparkJobRequest request) {
        logger.entry(process, request);

        // Execute job script
        final JerseyRestClient client = sparkLivyProcessManager.getClient(process);
        final String script = scalaScriptService.wrapScriptForLivy(request);
        final Statement statement = submitCode(client, script, process);

        final String jobId = ScalaScriptService.newTableName();
        sparkLivyProcessManager.setStatementId(jobId, statement.getId());

        // Generate response
        final SparkJobResponse response = new SparkJobResponse();
        response.setId(jobId);
        response.setStatus(StatementStateTranslator.translate(statement.getState()));
        return logger.exit(response);
    }

    @Nonnull
    @Override
    public Optional<Response> downloadQuery(@Nonnull SparkShellProcess process, @Nonnull String queryId, @Nonnull String saveId) {
        throw new UnsupportedOperationException();
    }


    @Nonnull
    @Override
    public Optional<TransformResponse> getQueryResult(@Nonnull SparkShellProcess process, @Nonnull String id) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<SparkJobResponse> getJobResult(@Nonnull final SparkShellProcess process, @Nonnull final String id) {
        logger.entry(process, id);
        Validate.isInstanceOf(SparkLivyProcess.class, process, "SparkLivyRestClient.getJobResult called on non Livy Process");
        SparkLivyProcess sparkLivyProcess = (SparkLivyProcess) process;

        // Request result from Livy
        final JerseyRestClient client = sparkLivyProcessManager.getClient(process);
        final Integer statementId = sparkLivyProcessManager.getStatementId(id);

        final Statement statement = livyClient.getStatement(client, sparkLivyProcess, statementId);
        sparkLivyProcessManager.setStatementId(id, statement.getId());

        // Generate response
        final SparkJobResponse response = LivyRestModelTransformer.toJobResponse(id, statement);

        if (response.getStatus() != TransformResponse.Status.ERROR) {
            return logger.exit(Optional.of(response));
        } else {
            throw logger.throwing(new SparkException(String.format("Unexpected error found in transform response:\n%s",
                                                                   response.getMessage())));
        }
    }

    @Nonnull
    @Override
    public Optional<SaveResponse> getQuerySave(@Nonnull SparkShellProcess process, @Nonnull String
        queryId, @Nonnull String saveId) {
        throw new UnsupportedOperationException();
    }


    @Nonnull
    @Override
    public TransformResponse query(@Nonnull SparkShellProcess process, @Nonnull TransformRequest request) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public SaveResponse saveQuery(@Nonnull SparkShellProcess process, @Nonnull String id, @Nonnull SaveRequest
        request) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Optional<Response> downloadTransform(@Nonnull SparkShellProcess process, @Nonnull String transformId, @Nonnull String saveId) {
        // 1. get "SaveResult" serialized from Livy
        logger.entry(process, transformId, saveId);

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        String script = scriptGenerator.script("getSaveResult", ScalaScriptUtils.scalaStr(saveId));

        Statement statement = submitCode(client, script, process);

        if (statement.getState() == StatementState.running
            || statement.getState() == StatementState.waiting) {
            statement = pollStatementForever(client, process, statement.getId());
        } else {
            throw logger.throwing(new LivyException("Unexpected error"));
        }

        URI uri = LivyRestModelTransformer.toUri(statement);
        SaveResult result = new SaveResult(new Path(uri));

        // 2. Create a response with data from filesysem
        if (result.getPath() != null) {
            Optional<Response> response =
                Optional.of(
                    Response.ok(new ZipStreamingOutput(result.getPath(), fileSystem))
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM)
                        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + saveId + ".zip\"")
                        .build());
            return logger.exit(response);
        } else {
            return logger.exit(Optional.of(
                createErrorResponse(Response.Status.NOT_FOUND, "download.notFound"))
            );
        }

    }


    @Nonnull
    @Override
    public DataSources getDataSources(@Nonnull SparkShellProcess process) {
        logger.entry(process);

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        String script = scriptGenerator.script("getDataSources");

        Statement statement = submitCode(client, script, process);

        if (statement.getState() == StatementState.running
            || statement.getState() == StatementState.waiting) {
            statement = pollStatementForever(client, process, statement.getId());
        } else {
            throw new LivyException("Unexpected error");
        }

        return logger.exit(LivyRestModelTransformer.toDataSources(statement));
    }


    @Nonnull
    @Override
    public Optional<TransformResponse> getTransformResult(@Nonnull SparkShellProcess process, @Nonnull String
        transformId) {
        logger.entry(process, transformId);
        long startMillis = System.currentTimeMillis();

        Validate.isInstanceOf(SparkLivyProcess.class, process, "SparkLivyRestClient.getTransformResult called on non Livy Process");
        SparkLivyProcess sparkLivyProcess = (SparkLivyProcess) process;

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        TransformResponse response;
        try {
            Integer stmtId = Integer.valueOf(transformId);
            // transformId is a stmtId
            Statement statement = livyClient.pollStatement(client, sparkLivyProcess, stmtId);
            // TODO: result from statement could still be a pending transform, or require more polling...
            //   if statement contains a result we will get it in response, otherwise it will get set to stmtId
            response = LivyRestModelTransformer.toTransformResponse(statement, statement.getId().toString());
        } catch (NumberFormatException e) {
            // transformId is from some previous transform, meaning it came from transformService but wasn't ready yet
            //  ie. The result came back from Livy, but the transform result still needs to be fetched
            String script = scriptGenerator.script("getTransform", ScalaScriptUtils.scalaStr(transformId));

            Statement statement = submitCode(client, script, process);

            long timeSpent = System.currentTimeMillis() - startMillis;
            if (timeSpent + livyProperties.getPollingInterval() < livyProperties.getPollingLimit()) {
                // still time left in the configured polling policy to do more polling, see if we can get the response from the transform service
                statement = pollStatement(client, sparkLivyProcess, statement.getId(), livyProperties.getPollingLimit() - timeSpent);
            }

            // response.tableName will be Livy ID if no additional polling was done.
            //    Otherwise, it will be from the result of statement, and may need to be checked again if transformResponse is PENDING.
            response = LivyRestModelTransformer.toTransformResponse(statement, statement.getId().toString());

            if (response.getStatus() == TransformResponse.Status.ERROR) {
                throw new LivyException(String.format("Unexpected error found in transform response:\n%s", response.getMessage()));
            } // end if
        } // end catch

        return logger.exit(Optional.of(response));
    }

    @Nonnull
    @Override
    public Optional<SaveResponse> getTransformSave(@Nonnull SparkShellProcess process, @Nonnull String
        transformId, @Nonnull String saveId) {
        logger.entry(process, transformId, saveId);
        Validate.isInstanceOf(SparkLivyProcess.class, process, "SparkLivyRestClient.getTransformSave called on non Livy Process");
        SparkLivyProcess sparkLivyProcess = (SparkLivyProcess) process;

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        SaveResponse response;
        Integer stmtId = transformIdsToLivyId.getIfPresent(transformId);
        if (stmtId != null) {
            Statement statement = livyClient.getStatement(client, sparkLivyProcess, stmtId);
            response = LivyRestModelTransformer.toSaveResponse(statement);
            if (statement.getState() == StatementState.available) {
                transformIdsToLivyId.invalidate(transformId);
            }
        } else {
            String script = scriptGenerator.script("getSave", saveId);

            Statement statement = submitCode(client, script, process);
            response = LivyRestModelTransformer.toSaveResponse(statement);
            response.setId(saveId);
            // remember the true id.  how? a cache.. it's not gonna get communicated back to us from UI..
            transformIdsToLivyId.put(transformId, statement.getId());
        }

        return logger.exit(Optional.of(response));
    }


    @Nonnull
    @Override
    public SaveResponse saveTransform(@Nonnull SparkShellProcess process, @Nonnull String
        transformId, @Nonnull SaveRequest request) {
        logger.entry(process, transformId, request);

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        String script = scriptGenerator.wrappedScript("submitSaveJob", "", "\n", ScalaScriptUtils.toJsonInScalaString(request), transformId);

        Statement statement = submitCode(client, script, process);
        statement = pollStatementForever(client, process, statement.getId());
        SaveResponse saveResponse = LivyRestModelTransformer.toSaveResponse(statement);

        transformIdsToLivyId.put(transformId, statement.getId());

        return logger.exit(saveResponse);
    }

    private Statement submitCode(JerseyRestClient client, String script, SparkShellProcess process) {
        Validate.isInstanceOf(SparkLivyProcess.class, process, "SparkLivyRestClient.submitCode called on non Livy Process");
        SparkLivyProcess sparkLivyProcess = (SparkLivyProcess) process;

        StatementsPost sp = new StatementsPost.Builder().code(script).kind("spark").build();

        return livyClient.postStatement(client, sparkLivyProcess, sp);
    }


    @Nonnull
    @Override
    public TransformResponse transform(@Nonnull SparkShellProcess process, @Nonnull TransformRequest request) {
        logger.entry(process, request);

        Validate.isInstanceOf(SparkLivyProcess.class, process, "SparkLivyRestClient.transform called on non Livy Process");
        SparkLivyProcess sparkLivyProcess = (SparkLivyProcess) process;

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        String transformId = ScalaScriptService.newTableName();
        String script = scalaScriptService.wrapScriptForLivy(request, transformId);

        Statement statement = submitCode(client, script, process);

        // check the server for script result.  If polling limit reached just return to UI and let it decide if it wants to check again
        statement = pollStatement(client, process, statement.getId());
        livyIdToTransformId.put(statement.getId(),transformId);

        TransformResponse response = LivyRestModelTransformer.toTransformResponse(statement, transformId);

        if (!StatementState.READY_STATES.contains(statement.getState())) {
            // result from Livy is not ready and a new transform id was created.. don't bother with that one.. let's
            response.setTable(statement.getId().toString());
            return response;
        }

        // Result from Livy is READY!  it either contains the complete query results or a pending transform that needs further checking

        if (StatementState.READY_STATES.contains(statement.getState()) && response.getStatus() == TransformResponse.Status.PENDING) {
            // statement is READY and transformResponse is PENDING...
            //   so we need to keep checking  TODO: only check for the amount of time remaining...
            Optional<TransformResponse> optResponse = getTransformResult(sparkLivyProcess, response.getTable());
            response = optResponse.orElseThrow(() -> new LivyException("should not receive no response"));
            return logger.exit(response);
        }

        // statement is READY, and is not a pending transform.
        return response;
    }


    @Nonnull
    public TransformResponse kyloCatalogTransform(@Nonnull final SparkShellProcess process, @Nonnull final KyloCatalogReadRequest request) {
        logger.entry(process, request);

        String script = scriptGenerator.wrappedScript("kyloCatalogTransform", "", "\n", ScalaScriptUtils.toJsonInScalaString(request));
        logger.debug("scala str\n{}", script);

        JerseyRestClient client = sparkLivyProcessManager.getClient(process);

        Statement statement = submitCode(client, script, process);

        if (statement.getState() == StatementState.running
            || statement.getState() == StatementState.waiting) {
            statement = pollStatementForever(client, process, statement.getId());
        } else {
            throw logger.throwing(new LivyException("Unexpected error"));
        }

        // call with null so a transformId will be generated for this query
        return logger.exit(LivyRestModelTransformer.toTransformResponse(statement, null));
    }


    @Nonnull
    @Override
    public ServerStatusResponse serverStatus(SparkShellProcess sparkShellProcess) {
        logger.entry(sparkShellProcess);

        if (sparkShellProcess instanceof SparkLivyProcess) {
            return logger.exit(LivyRestModelTransformer.toServerStatusResponse(livyServer, ((SparkLivyProcess) sparkShellProcess).getSessionId()));
        } else {
            throw logger.throwing(new IllegalStateException("SparkLivyRestClient.serverStatus called on non Livy Process"));
        }
    }


    /**
     * Polls indefinitely
     */
    @VisibleForTesting
    Statement pollStatementForever(JerseyRestClient jerseyClient, SparkShellProcess sparkShellProcess, Integer stmtId) {
        Validate.isInstanceOf(SparkLivyProcess.class, sparkShellProcess, "SparkLivyRestClient.pollStatementForever called on non Livy Process");
        SparkLivyProcess sparkLivyProcess = (SparkLivyProcess) sparkShellProcess;

        return livyClient.pollStatement(jerseyClient, sparkLivyProcess, stmtId);
    }

    /**
     * Polls for the configured amount of time.
     */
    Statement pollStatement(JerseyRestClient jerseyClient, SparkShellProcess sparkShellProcess, Integer stmtId) {
        Validate.isInstanceOf(SparkLivyProcess.class, sparkShellProcess, "SparkLivyRestClient.pollStatementForever called on non Livy Process");
        SparkLivyProcess sparkLivyProcess = (SparkLivyProcess) sparkShellProcess;

        return livyClient.pollStatement(jerseyClient, sparkLivyProcess, stmtId, 0L);
    }

    /**
     * Polls for the time given
     */
    private Statement pollStatement(JerseyRestClient jerseyClient, SparkShellProcess sparkShellProcess, Integer stmtId, Long wait) {
        Validate.isInstanceOf(SparkLivyProcess.class, sparkShellProcess, "SparkLivyRestClient.pollStatement called on non Livy Process");
        SparkLivyProcess sparkLivyProcess = (SparkLivyProcess) sparkShellProcess;

        return livyClient.pollStatement(jerseyClient, sparkLivyProcess, stmtId, wait);
    }

    /**
     * Creates a response for the specified message.
     *
     * @param status  the response status
     * @param message the message text
     * @return the response
     */
    private static Response createErrorResponse(@Nonnull final Response.Status status, @Nonnull final String message) {
        final TransformResponse entity = new TransformResponse();
        entity.setMessage(message);
        entity.setStatus(TransformResponse.Status.ERROR);
        return Response.status(status).header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON).entity(entity).build();
    }

}
