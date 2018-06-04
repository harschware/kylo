package com.thinkbiganalytics.kylo.utils;

import com.thinkbiganalytics.kylo.model.enums.StatementState;
import com.thinkbiganalytics.spark.rest.model.SaveResponse;
import com.thinkbiganalytics.spark.rest.model.TransformResponse;

public class StatementStateTranslator {
    private StatementStateTranslator() {
    } // private constructor

    public static TransformResponse.Status translate(StatementState state) {
        switch (state) {
            case available:
                return TransformResponse.Status.SUCCESS;
            case error:
            case cancelled:
            case cancelling:
                return TransformResponse.Status.ERROR;
            default:
                return TransformResponse.Status.PENDING;
        }
    }

    public static SaveResponse.Status translateToSaveResponse(StatementState state) {
        switch (state) {
            case available:
                return SaveResponse.Status.SUCCESS;
            case error:
            case cancelled:
            case cancelling:
                return SaveResponse.Status.ERROR;
            default:
                return SaveResponse.Status.PENDING;
        }
    }
}
