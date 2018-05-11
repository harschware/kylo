package com.thinkbiganalytics.kylo.model;

import com.fasterxml.jackson.databind.JsonNode;

public class StatementOutputResponse {
    private String status;
    private Integer execution_count;
    private JsonNode data;

    // default constructor
    public StatementOutputResponse() {
    }

    public String getStatus() {
        return status;
    }

    public Integer getExecution_count() {
        return execution_count;
    }

    public JsonNode getData() {
        return data;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StatementOutputResponse{");
        sb.append("status='").append(status).append('\'');
        sb.append(", execution_count=").append(execution_count);
        sb.append(", data=").append(data);
        sb.append('}');
        return sb.toString();
    }

    StatementOutputResponse(StatementOutputResponse.Builder sgb) {
        this.status = sgb.status;
        this.execution_count = sgb.execution_count;
        this.data = data;
    }

    public static class Builder {
        private String status;
        private Integer execution_count;
        private JsonNode data;


        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public Builder execution_count(Integer execution_count) {
            this.execution_count = execution_count;
            return this;
        }

        public Builder data(JsonNode data) {
            this.data = data;
            return this;
        }

        public StatementOutputResponse build() {
            return new StatementOutputResponse(this);
        }
    }

}

