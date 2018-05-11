package com.thinkbiganalytics.kylo.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbiganalytics.discovery.schema.QueryResultColumn;
import com.thinkbiganalytics.kylo.model.StatementsPostResponse;
import com.thinkbiganalytics.kylo.model.TestSerializing;
import com.thinkbiganalytics.kylo.model.enums.StatementState;
import com.thinkbiganalytics.spark.rest.model.TransformQueryResult;
import com.thinkbiganalytics.spark.rest.model.TransformResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.utils.TestUtils;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLivyRestModelTransformer {
    private static final Logger logger = LoggerFactory.getLogger(TestSerializing.class);

    @Test
    public void simpleTest() throws IOException {
        final String json = TestUtils.getTestResourcesFileAsString("dataFrameStatementPostResponse.json");

        //JSON from String to Object
        StatementsPostResponse statementsPostResponse = new ObjectMapper().readValue(json, StatementsPostResponse.class);
        logger.info("response={}", statementsPostResponse);

        TransformResponse response = LivyRestModelTransformer.toTransformResponse(statementsPostResponse);
        assertThat(response).hasFieldOrPropertyWithValue("status", TransformResponse.Status.SUCCESS );
        assertThat(response).hasFieldOrPropertyWithValue("progress", 1.0);

        TransformQueryResult tqr = response.getResults();
        assertThat(tqr).isNotNull();

        List<QueryResultColumn> cols = tqr.getColumns();
        assertThat(cols).isNotNull();
        String dt = cols.get(0).getDataType();
        String dName = cols.get(0).getDisplayName();
    }
}
