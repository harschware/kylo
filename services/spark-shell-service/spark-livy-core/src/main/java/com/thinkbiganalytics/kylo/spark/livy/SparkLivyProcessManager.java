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
import com.thinkbiganalytics.kylo.exceptions.LivyException;
import com.thinkbiganalytics.kylo.model.Session;
import com.thinkbiganalytics.kylo.model.SessionsGet;
import com.thinkbiganalytics.kylo.model.SessionsGetResponse;
import com.thinkbiganalytics.rest.JerseyClientConfig;
import com.thinkbiganalytics.rest.JerseyRestClient;
import com.thinkbiganalytics.spark.rest.model.RegistrationRequest;
import com.thinkbiganalytics.spark.shell.SparkShellProcess;
import com.thinkbiganalytics.spark.shell.SparkShellProcessListener;
import com.thinkbiganalytics.spark.shell.SparkShellProcessManager;
import com.thinkbiganalytics.spark.shell.SparkShellRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SparkLivyProcessManager implements SparkShellProcessManager {
    private static final Logger logger = LoggerFactory.getLogger(SparkLivyProcessManager.class);

    List<SparkShellProcessListener> listeners = Lists.newArrayList();

    SparkShellProcess sparkProcess = SparkLivyProcess.newInstance();

    /**
     * Map of Spark Shell processes to Jersey REST clients
     */
    @Nonnull
    private final Map<SparkShellProcess, JerseyRestClient> clients = new HashMap<>();

    @Nonnull
    private final Map<SparkShellProcess, Integer> clientSessionCache = new HashMap<>();

    @Nonnull
    private final Map<Integer, Integer> stmntIdCache = new HashMap<>();


    @Resource
    private SparkShellRestClient restClient;

    @Override
    public void addListener(@Nonnull SparkShellProcessListener listener) {
        logger.debug("adding listener '{}", listener);
        listeners.add(listener);
    }

    @Override
    public void removeListener(@Nonnull SparkShellProcessListener listener) {
        logger.debug("removing listener '{}", listener);
        listeners.remove(listener);
    }

    @Nonnull
    @Override
    public SparkShellProcess getProcessForUser(@Nonnull String username) throws InterruptedException {
        // TODO: make user aware
        return sparkProcess;
    }

    @Nonnull
    @Override
    public SparkShellProcess getSystemProcess() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void register(@Nonnull String clientId, @Nonnull RegistrationRequest registration) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets or creates a Jersey REST client for the specified Spark Shell process.
     *
     * @param process the Spark Shell process
     * @return the Jersey REST client
     */
    @Nonnull
    public JerseyRestClient getClient(@Nonnull final SparkShellProcess process) {
        return clients.computeIfAbsent(process, target -> {
            final JerseyClientConfig config = new JerseyClientConfig();
            config.setHost(target.getHostname());
            config.setPort(target.getPort());

            return new JerseyRestClient(config);
        });
    }

    @Override
    public void start(@Nonnull String username) {
        logger.info("JerseyClient='{}'", restClient);

        SessionsGet sg = new SessionsGet.Builder().build();
        JerseyRestClient jerseyClient = getClient(sparkProcess);

        // fetch or create new server session
        Session currentSession;
        if (clientSessionCache.containsKey(sparkProcess)) {
            SessionsGetResponse sessions = jerseyClient.get("/sessions", null, SessionsGetResponse.class);
            logger.info("sessions={}", sessions);

            currentSession = sessions.getSessionWithId(clientSessionCache.get(sparkProcess)).orElseThrow(
                    () -> new LivyException("Client session no longer exists in LivyServer")
            );
        } else {
            currentSession = jerseyClient.post("/sessions", sg, Session.class);
            clientSessionCache.put(sparkProcess, currentSession.getId());
            for (SparkShellProcessListener listener : listeners) {
                listener.processStarted(sparkProcess);
            }
        }

        Integer currentSessionId = currentSession.getId();
        if (!currentSession.getState().equals("idle")) {
            logger.info("Created session with id='{}', but it was returned with state != idle, state = '{}'", currentSession.getId(), currentSession.getState());
            waitForSessionToBecomeIdle(jerseyClient, currentSessionId);
        } // end if

        if (sparkProcess != null) {
            for (SparkShellProcessListener listener : listeners) {
                listener.processReady(sparkProcess);
            }
        }
    }

    private void waitForSessionToBecomeIdle(JerseyRestClient jerseyClient, Integer id) {
        Optional<Session> optSession;
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            SessionsGetResponse sessions = jerseyClient.get("/sessions", null, SessionsGetResponse.class);
            logger.info("sessions={}", sessions);

            logger.info("poll server for session with id='{}'", id);
            optSession = sessions.getSessionWithId(id);
        } while (!(optSession.isPresent() && optSession.get().getState().equals("idle")));
    }

    @Nonnull
    public Integer getLivySessionId( @Nonnull  SparkShellProcess process ) {
        return clientSessionCache.get(process);
    }

    @Nonnull
    public Integer getLastStatementId( @Nonnull SparkShellProcess process ) {
        return stmntIdCache.get(clientSessionCache.get(process));
    }

    @Nonnull
    public Integer incrementStatementId( @Nonnull SparkShellProcess process ) {
        Integer curSession = clientSessionCache.get(process);
        Integer curId = stmntIdCache.get(curSession);

        if( curId == null ) {
            stmntIdCache.put( curSession,0 );
            return 0;
        } else {
            stmntIdCache.put( curSession, ++curId );
            return curId;
        }
    }

}
