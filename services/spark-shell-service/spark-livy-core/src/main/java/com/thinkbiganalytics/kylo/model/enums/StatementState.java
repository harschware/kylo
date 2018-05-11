package com.thinkbiganalytics.kylo.model.enums;

/**
 * Statement State
 * Value	Description
 * waiting	Statement is enqueued but execution hasn't started
 * running	Statement is currently running
 * available	Statement has a response ready
 * error	Statement failed
 * cancelling	Statement is being cancelling
 * cancelled	Statement is cancelled
 */
public enum StatementState {
    waiting,
    running,
    available,
    error,
    cancelling,
    cancelled
}

