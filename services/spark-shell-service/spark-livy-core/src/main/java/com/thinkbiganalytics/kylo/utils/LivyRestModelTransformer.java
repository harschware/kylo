package com.thinkbiganalytics.kylo.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.thinkbiganalytics.discovery.model.DefaultQueryResultColumn;
import com.thinkbiganalytics.discovery.schema.QueryResultColumn;
import com.thinkbiganalytics.kylo.model.StatementOutputResponse;
import com.thinkbiganalytics.kylo.model.StatementsPostResponse;
import com.thinkbiganalytics.spark.rest.model.TransformQueryResult;
import com.thinkbiganalytics.spark.rest.model.TransformResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LivyRestModelTransformer {
    private static final Logger logger = LoggerFactory.getLogger(LivyRestModelTransformer.class);

    private LivyRestModelTransformer() {
    } // static methods only

    public static TransformResponse toTransformResponse(StatementsPostResponse spr) {
        TransformResponse response = new TransformResponse();

        switch (spr.getState()) {
            case available:
                response.setStatus(TransformResponse.Status.SUCCESS);
                break;
            case error:
                response.setStatus(TransformResponse.Status.ERROR);
                break;
            default:
                response.setStatus(TransformResponse.Status.PENDING);
                break;
        }
        response.setProgress(spr.getProgress());

        response.setResults(toTransformQueryResult(spr.getOutput()));

        return response;
    }

    public static TransformQueryResult toTransformQueryResult(StatementOutputResponse sor) {
        TransformQueryResult tqr = new TransformQueryResult();

        JsonNode data = sor.getData();
        logger.info("data={}", data);
        if (!sor.getStatus().equalsIgnoreCase("ok")) {
            return tqr;
        }
        List<QueryResultColumn> resColumns = Lists.newArrayList();
        tqr.setColumns(resColumns);

        ArrayNode json = (ArrayNode) data.get("application/json");
        int numElements = 0;
        Iterator<JsonNode> rowIter = json.elements();
        while (rowIter.hasNext()) {
            ObjectNode row = (ObjectNode) rowIter.next();
            if (numElements++ == 0) {
                //  build column metadata
                logger.debug("build column metadata");
                ArrayNode cols = (ArrayNode) row.get("schema");
                Iterator<JsonNode> colObjsIter = cols.elements();

                int idx = 0;
                while (colObjsIter.hasNext()) {
                    ObjectNode colObj = (ObjectNode) colObjsIter.next();
                    final JsonNode dataType = colObj.get("dataType");
                    JsonNode metadata = colObj.get("metadata");
                    String name = colObj.get("name").asText();
                    String nullable = colObj.get("nullable").asText();  // "true"|"false"

                    QueryResultColumn qrc = new DefaultQueryResultColumn();
                    qrc.setDisplayName(name);
                    qrc.setField(name);
                    qrc.setIndex(idx++);
                    qrc.setDataType("string"); // dataType is always empty:: https://www.mail-archive.com/user@livy.incubator.apache.org/msg00262.html
                    resColumns.add(qrc);
                }
            } // end if

            // get row data
            logger.debug("build row data");

            ArrayNode values = (ArrayNode) row.get("values");
            Iterator<JsonNode> valuesIter = values.elements();

            List<List<Object>> rowData = Lists.newArrayListWithCapacity(resColumns.size());

            int rowNum = 0;
            while (valuesIter.hasNext()) {
                    JsonNode valueNode = (JsonNode) valuesIter.next();
                    List<Object> newValues = Lists.newArrayList();
                    valueNode.get(0);
                }
                logger.debug("{}", rowData);
            }


        //tqr.setRows(null);
        //tqr.setValidationResults(null);

        //String status = sor.getStatus();
        return null;
    }
}
