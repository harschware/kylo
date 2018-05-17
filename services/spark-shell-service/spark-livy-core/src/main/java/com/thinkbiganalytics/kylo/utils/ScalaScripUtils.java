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

public class ScalaScripUtils {
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
        String script = request.getScript();

        StringBuilder sb = new StringBuilder();
        if( request.getParent() != null ) {
            sb.append(request.getParent().getScript());
            sb.append("var parent = df\n\n");
        } // end if

        sb.append(wrapScriptWithPaging(script, request.getPageSpec()));
        sb.append("%json dfRows\n");
        return sb.toString();
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
        sb.append( " ) " );
        sb.append(startCol);
        sb.append(" else lastCol\n");
        sb.append("val dfStopCol = if( lastCol >= ");
        sb.append(stopCol);
        sb.append( " ) " );
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
    
}
