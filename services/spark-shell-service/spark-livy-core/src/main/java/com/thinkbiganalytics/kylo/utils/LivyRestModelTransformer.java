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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.thinkbiganalytics.discovery.model.DefaultQueryResultColumn;
import com.thinkbiganalytics.discovery.schema.QueryResultColumn;
import com.thinkbiganalytics.kylo.exceptions.LivyCodeException;
import com.thinkbiganalytics.kylo.exceptions.LivyException;
import com.thinkbiganalytics.kylo.exceptions.LivySerializationException;
import com.thinkbiganalytics.kylo.model.Statement;
import com.thinkbiganalytics.kylo.model.StatementOutputResponse;
import com.thinkbiganalytics.kylo.model.enums.StatementOutputStatus;
import com.thinkbiganalytics.kylo.model.enums.StatementState;
import com.thinkbiganalytics.spark.dataprofiler.model.MetricType;
import com.thinkbiganalytics.spark.dataprofiler.output.OutputRow;
import com.thinkbiganalytics.spark.rest.model.DataSources;
import com.thinkbiganalytics.spark.rest.model.SaveResponse;
import com.thinkbiganalytics.spark.rest.model.TransformQueryResult;
import com.thinkbiganalytics.spark.rest.model.TransformResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

public class LivyRestModelTransformer {
    private static final Logger logger = LoggerFactory.getLogger(LivyRestModelTransformer.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    private LivyRestModelTransformer() {
    } // static methods only

    public static TransformResponse toTransformResponse(Statement statement, String transformId) {
        TransformResponse response = prepTransformResponse(statement, transformId);

        if (response.getStatus() == TransformResponse.Status.SUCCESS) {
            String code = statement.getCode();
            if (code.endsWith("dfRows\n")) {
                response.setResults(toTransformQueryResultWithSchema(statement.getOutput()));
            } else if (code.endsWith("dfProf")) {
                List<OutputRow> rows = toTransformResponseProfileStats(statement.getOutput());
                response.setProfile(toTransformResponseProfileStats(statement.getOutput()));
                response.setActualCols(1);
                Integer actualRows = rows.stream()
                        .filter(metric -> metric.getMetricType().equals(MetricType.TOTAL_COUNT))
                        .map(metric -> Integer.valueOf(metric.getMetricValue()))
                        .findFirst().orElse(1);
                response.setActualRows(actualRows);
                response.setResults(emptyResult());
            } else {
                throw new LivyException("Unsupported result type requested of Livy.  Results not recognized");
            } // end if
        }

        return response;
    }



    private static TransformQueryResult toTransformQueryResultWithSchema(StatementOutputResponse sor) {
        checkCodeWasWellFormed(sor);

        TransformQueryResult tqr = new TransformQueryResult();

        JsonNode data = sor.getData();
        List<QueryResultColumn> resColumns = Lists.newArrayList();
        tqr.setColumns(resColumns);

        ArrayNode json = (ArrayNode) data.get("application/json");
        int numRows = 0;

        Iterator<JsonNode> rowIter = json.elements();
        List<List<Object>> rowData = Lists.newArrayList();
        while (rowIter.hasNext()) {
            JsonNode row = rowIter.next();
            if (numRows++ == 0) {
                String jsonString = row.asText();
                try {
                    JsonNode actualObj = mapper.readTree(jsonString);
                    row = actualObj;
                } catch (IOException e) {
                    throw new LivyException("Unable to read schema JSON structure returned from Livy"); // TODO: specialize me
                } // end try/catch

                //  build column metadata
                logger.debug("build column metadata");
                String type = row.get("type").asText();
                if (type.equals("struct")) {
                    ArrayNode fields = (ArrayNode) row.get("fields");

                    Iterator<JsonNode> colObjsIter = fields.elements();

                    int idx = 0;
                    while (colObjsIter.hasNext()) {
                        ObjectNode colObj = (ObjectNode) colObjsIter.next();
                        final JsonNode dataType = colObj.get("type");
                        JsonNode metadata = colObj.get("metadata");
                        String name = colObj.get("name").asText();
                        String nullable = colObj.get("nullable").asText();  // "true"|"false"


                        QueryResultColumn qrc = new DefaultQueryResultColumn();
                        qrc.setDisplayName(name);
                        qrc.setField(name);
                        qrc.setHiveColumnLabel(name);  // not used, but still be expected to be unique
                        qrc.setIndex(idx++);
                        qrc.setDataType(dataType.asText()); // dataType is always empty:: https://www.mail-archive.com/user@livy.incubator.apache.org/msg00262.html
                        qrc.setComment(metadata.asText());
                        resColumns.add(qrc);
                    }
                } // will there be types other than "struct"?
                continue;
            } // end schema extraction

            // get row data
            logger.debug("build row data");
            ArrayNode valueRows = (ArrayNode) row;

            Iterator<JsonNode> valuesIter = valueRows.elements();
            while (valuesIter.hasNext()) {
                ArrayNode valueNode = (ArrayNode) valuesIter.next();
                Iterator<JsonNode> valueNodes = valueNode.elements();
                List<Object> newValues = Lists.newArrayListWithCapacity(resColumns.size());
                int colCount = 0;
                while (valueNodes.hasNext()) {
                    JsonNode value = valueNodes.next();
                    QueryResultColumn qrc = resColumns.get(colCount++);
                    // extract values according to the schema that was communicated by spark
                    switch (qrc.getDataType()) {
                        case "integer":
                            newValues.add(value.asInt());
                            break;
                        default:
                            newValues.add(value.asText());
                            break;
                    }
                } // end while
                rowData.add(newValues);
            } // end of valueRows
        } // end sor.data
        logger.trace("rowData={}", rowData);
        tqr.setRows(rowData);
        //tqr.setValidationResults(null);

        return tqr;
    }


    private static List<OutputRow> toTransformResponseProfileStats(StatementOutputResponse sor) {
        checkCodeWasWellFormed(sor);

        JsonNode data = sor.getData();
        ArrayNode json = (ArrayNode) data.get("application/json");
        final List<OutputRow> profileResults = Lists.newArrayList();

        Iterator<JsonNode> rowIter = json.elements();
        while (rowIter.hasNext()) {
            JsonNode row = rowIter.next();
            String columnName = row.get("columnName").asText();
            String metricType = row.get("metricType").asText();
            String metricValue = row.get("metricValue").asText();
            OutputRow outputRow = new OutputRow(columnName, metricType, metricValue);
            profileResults.add(outputRow);
        } // end rowIter.next

        return profileResults;
    }

    public static SaveResponse toSaveResponse(Statement statement) {

        StatementOutputResponse sor = statement.getOutput();

        checkCodeWasWellFormed(sor);

        if (statement.getState() != StatementState.available) {
            SaveResponse response = new SaveResponse();
            response.setId(statement.getId().toString());
            response.setStatus(StatementStateTranslator.translateToSaveResponse(statement.getState()));
            return response;
        }

        return serializeStatementOutputResponse(sor, SaveResponse.class);
    }


    private static TransformResponse prepTransformResponse(Statement statement, String transformId) {
        TransformResponse response = new TransformResponse();

        TransformResponse.Status status = StatementStateTranslator.translate(statement.getState());
        response.setStatus(status);
        response.setProgress(statement.getProgress());
        response.setTable(transformId);
        return response;
    }


    private static TransformQueryResult emptyResult() {
        TransformQueryResult tqr = new TransformQueryResult();
        tqr.setColumns(Lists.newArrayList());
        tqr.setRows(Lists.newArrayList());
        return tqr;
    }

    public static DataSources toDataSources(Statement statement) {
        StatementOutputResponse sor = statement.getOutput();
        checkCodeWasWellFormed(sor);

        return serializeStatementOutputResponse(sor, DataSources.class);
    }


    public static URI toUri(Statement statement) {
        StatementOutputResponse sor = statement.getOutput();
        checkCodeWasWellFormed(sor);

        return serializeStatementOutputResponse(sor, URI.class);
    }

    private static <T extends Object> T serializeStatementOutputResponse(StatementOutputResponse sor, Class<T> clazz) {
        String errMsg = String.format("Unable to deserialize %s returned from Livy", clazz.getSimpleName());

        JsonNode data = sor.getData();
        if (data != null) {
            JsonNode json = data.get("application/json");
            String jsonString = json.asText();
            try {
                T clazzInstance = mapper.readValue(jsonString, clazz);
                return clazzInstance;
            } catch (IOException e) {
                throw new LivySerializationException(errMsg);
            } // end try/catch
        } else {
            throw new LivySerializationException(errMsg);
        }
    }

    private static void checkCodeWasWellFormed(StatementOutputResponse statementOutputResponse) {
        if (statementOutputResponse != null && statementOutputResponse.getStatus() != StatementOutputStatus.ok) {
            String msg = String.format("Malformed code sent to Livy.  ErrorType='%s', Error='%s', Traceback='%s'",
                    statementOutputResponse.getEname(),
                    statementOutputResponse.getEvalue(),
                    statementOutputResponse.getTraceback());
            throw new LivyCodeException(msg);
        }
    }

}
