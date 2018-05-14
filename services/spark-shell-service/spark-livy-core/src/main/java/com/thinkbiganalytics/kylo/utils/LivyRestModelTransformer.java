package com.thinkbiganalytics.kylo.utils;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.thinkbiganalytics.discovery.model.DefaultQueryResultColumn;
import com.thinkbiganalytics.discovery.schema.QueryResultColumn;
import com.thinkbiganalytics.kylo.exceptions.LivyCodeException;
import com.thinkbiganalytics.kylo.model.Statement;
import com.thinkbiganalytics.kylo.model.StatementOutputResponse;
import com.thinkbiganalytics.kylo.model.enums.StatementOutputStatus;
import com.thinkbiganalytics.spark.rest.model.TransformQueryResult;
import com.thinkbiganalytics.spark.rest.model.TransformResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class LivyRestModelTransformer {
    private static final Logger logger = LoggerFactory.getLogger(LivyRestModelTransformer.class);

    private LivyRestModelTransformer() {
    } // static methods only

    public static TransformResponse toTransformResponse(Statement spr) {
        TransformResponse response = new TransformResponse();

        TransformResponse.Status status = TranslateStatementStateToTransformStatus.translate(spr.getState());
        response.setStatus(status);
        response.setProgress(spr.getProgress());

        response.setResults(toTransformQueryResult(spr.getOutput()));
        response.setTable("noTableWhatsoever");

        return response;
    }

    public static TransformQueryResult toTransformQueryResult(StatementOutputResponse sor) {
        TransformQueryResult tqr = new TransformQueryResult();

        JsonNode data = sor.getData();
        logger.debug("data={}", data);
        if ( sor.getStatus() != StatementOutputStatus.ok) {
            String msg = String.format("Malfor==ent to Livy.  ErrorType='%s', Error='%s', Traceback='%s'",
                    sor.getEname(), sor.getEvalue(), sor.getTraceback());
            throw new LivyCodeException(msg);
        }
        List<QueryResultColumn> resColumns = Lists.newArrayList();
        tqr.setColumns(resColumns);

        ArrayNode json = (ArrayNode) data.get("application/json");
        int numRows = 0;
        List<List<Object>> rowData = Lists.newArrayListWithCapacity(resColumns.size());

        Iterator<JsonNode> rowIter = json.elements();
        while (rowIter.hasNext()) {
            ObjectNode row = (ObjectNode) rowIter.next();
            if (numRows++ == 0) {
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
                    qrc.setHiveColumnLabel(name);  // not used, but still be expected to be unique
                    qrc.setIndex(idx++);
                    qrc.setDataType("string"); // dataType is always empty:: https://www.mail-archive.com/user@livy.incubator.apache.org/msg00262.html
                    resColumns.add(qrc);
                }
            }

            // get row data
            logger.debug("build row data");

            ArrayNode values = (ArrayNode) row.get("values");

            List<Object> newValues = Lists.newArrayList();
            Iterator<JsonNode> valuesIter = values.elements();
            while (valuesIter.hasNext()) {
                JsonNode valueNode = (JsonNode) valuesIter.next();
                Object value = valueNode.asText();
                newValues.add(value);
            }
            rowData.add(newValues);
        }
        logger.debug("{}", rowData);


        tqr.setRows(rowData);
        //tqr.setValidationResults(null);

        return tqr;
    }
}
