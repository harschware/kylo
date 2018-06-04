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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkbiganalytics.spark.rest.model.PageSpec;
import com.thinkbiganalytics.spark.rest.model.TransformRequest;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ScalaScripUtils {

    // TODO: put elsewhere, cache correctly per user
    private static Map<String, Integer> scriptCache = new HashMap<String /*script*/, Integer /*varname*/>();
    private static Integer counter = 0;
    private static Pattern dfPattern = Pattern.compile("^df$", Pattern.MULTILINE);


    /**
     * Cache of transformatIds
     */
    @Nonnull
    public final static Cache<TransformRequest, String> transformCache = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .maximumSize(100)
            .build();

    private ScalaScripUtils() {
    } // static methods only


    /**
     * Modifies the script in request (assumed that it contains a dataframe named df), and creates a
     * List(schema,dataRows) object.  schema is a scala string of json representing the schema.  dataRows
     * is a List of Lists of the row data.  The columns and rows are paged according to the data provided
     * in request.pageSpec.
     *
     * @param request
     * @return
     */
    public static String wrapScriptForLivy(TransformRequest request) {

        String newScript;
        if( request.isDoProfile() ) {
            newScript = profiledDataFrame(request);
        } else {
            newScript = dataFrameWithSchema(request);
        }

        return newScript;
    }

    private static String profiledDataFrame(TransformRequest request) {
        String script = request.getScript();

        StringBuilder sb = new StringBuilder();
        sb.append( setParentVar(request) );
        sb.append(script);
        sb.append("val dfProf = com.thinkbiganalytics.spark.dataprofiler.core.Profiler.profileDataFrame(sparkContextService,profiler,df)\n");
        sb.append("%json dfProf\n");

        return sb.toString();
    }

    private static String dataFrameWithSchema(TransformRequest request) {
        String script = request.getScript();

        StringBuilder sb = new StringBuilder();
        if (request.getParent() != null) {
            sb.append( setParentVar(request) );
        } // end if

        String transformId = ScalaScripUtils.newTableName();
        transformCache.put(request,transformId);
        script = dfPattern.matcher(script).replaceAll("var df" + counter + " = df; df" + counter + ".cache()" +
                ".registerTempTable( \"" + transformId + "\" )\n");

        sb.append(wrapScriptWithPaging(script, request.getPageSpec()));
        sb.append("%json dfRows\n");

        return sb.toString();
    }

    private static String setParentVar(TransformRequest request) {
        String parentScript = request.getParent().getScript();
        Integer varCount;
        if (scriptCache.containsKey(parentScript)) {
            varCount = scriptCache.get(parentScript);
        } else {
            scriptCache.put(parentScript, counter);
            varCount = counter++;
        }

        return "var parent = df" + varCount + "\n\n";
    }


    /**
     * Modifies the script passed in (assumed it contains a dataframe named df), and creates a List(schema,dataRows)
     * object.  schema is a scala string of json representing the schema.  dataRows is a List of Lists of the row data.
     * The columns and rows are paged according to the data provided in request.pageSpec
     *
     * @param script
     * @param pageSpec
     * @return
     */
    private static String wrapScriptWithPaging(String script, PageSpec pageSpec) {
        StringBuilder sb = new StringBuilder(script);

        if( pageSpec != null ) {
            Integer startCol = pageSpec.getFirstCol();
            Integer stopCol = pageSpec.getFirstCol() + pageSpec.getNumCols();
            Integer startRow = pageSpec.getFirstRow();
            Integer stopRow = pageSpec.getFirstRow() + pageSpec.getNumRows();

            sb.append("val lastCol = df.columns.length - 1\n");
            sb.append("val dfStartCol = if( lastCol >= ");
            sb.append(startCol);
            sb.append(" ) ");
            sb.append(startCol);
            sb.append(" else lastCol\n");
            sb.append("val dfStopCol = if( lastCol >= ");
            sb.append(stopCol);
            sb.append(" ) ");
            sb.append(stopCol);
            sb.append(" else lastCol\n");

            sb.append("df = df.select( dfStartCol to dfStopCol map df.columns map col: _*)\n");
            sb.append("val dfRows = List( df.schema.json, df.rdd.zipWithIndex.filter( pair => pair._2>=");
            sb.append(startRow);
            sb.append(" && pair._2<=");
            sb.append(stopRow);
            sb.append(").map(_._1).collect.map(x => x.toSeq) )\n");
        } else {
            sb.append("val dfRows = df\n");
        }
        return sb.toString();
    }

    public static String getInitScript() {
        return "val ctx = com.thinkbiganalytics.spark.LivyWrangler.createSpringContext(sc, sqlContext)\n" +
                "val profiler = ctx.getBean(classOf[com.thinkbiganalytics.spark.dataprofiler.Profiler])\n" +
                "val transformService = ctx.getBean(classOf[com.thinkbiganalytics.spark.service.TransformService])\n" +
                "val sparkContextService = ctx.getBean(classOf[com.thinkbiganalytics.spark.SparkContextService])\n" +
                "val converterService = ctx.getBean(classOf[com.thinkbiganalytics.spark.service.DataSetConverterService])\n" +
                "val sparkShellTransformController = ctx.getBean(classOf[com.thinkbiganalytics.spark.rest.SparkShellTransformController])\n" +
                "val mapper = new com.fasterxml.jackson.databind.ObjectMapper()\n";
    }

    /**
     * Generates a new, unique table name.
     *
     * @return the table name
     * @throws IllegalStateException if a table name cannot be generated
     */
    public static String newTableName() {
        for (int i = 0; i < 100; ++i) {
            final String name = UUID.randomUUID().toString();
            if (name.matches("^[a-fA-F].*")) {
                return name.replace("-", "");
            }
        }
        throw new IllegalStateException("Unable to generate a new table name");
    }

}
