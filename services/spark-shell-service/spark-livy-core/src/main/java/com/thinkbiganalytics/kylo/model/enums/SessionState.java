package com.thinkbiganalytics.kylo.model.enums;

/**
 * Session State
 * Value	Description
 * not_started	Session has not been started
 * starting	Session is starting
 * idle	Session is waiting for input
 * busy	Session is executing a statement
 * shutting_down	Session is shutting down
 * error	Session errored out
 * dead	Session has exited
 * success	Session is successfully stopped
 */
public enum SessionState {
    not_started,
    starting,
    idle,
    busy,
    shutting_down,
    error,
    dead,
    success
}
