package com.thinkbiganalytics.kylo.utils;

public class ScalaScripUtils {
    private ScalaScripUtils() {} // static methods only

    public static String dataFrameToJava( String script ) {
        return script.concat("\nval e = df.take(1000)\n%json e\n");
    }
}
