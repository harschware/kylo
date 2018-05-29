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

import com.thinkbiganalytics.spark.rest.model.PageSpec;
import com.thinkbiganalytics.spark.rest.model.TransformRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class ScalaScripUtils {

    // TODO: put elsewhere, cache correctly per user
    private static Map<String, Integer> scriptCache = new HashMap<String /*script*/, Integer /*varname*/>();
    private static Integer counter = 0;
    private static Pattern dfPattern = Pattern.compile("^df$", Pattern.MULTILINE);

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

        script = dfPattern.matcher(script).replaceAll("var df" + counter + " = df; df" + counter + ".cache().count()");

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
        return sb.toString();
    }

    public static String getInitScript() {
        return "import com.thinkbiganalytics.spark.dataprofiler.Profiler\n" +
                "import com.thinkbiganalytics.spark.SparkContextService\n" +
                "val ctx = com.thinkbiganalytics.spark.dataprofiler.core.Profiler.createSpringContext(sc, sqlContext)\n" +
                "val profiler = ctx.getBean(classOf[Profiler])\n" +
                "val sparkContextService = ctx.getBean(classOf[SparkContextService])\n";
    }
}
