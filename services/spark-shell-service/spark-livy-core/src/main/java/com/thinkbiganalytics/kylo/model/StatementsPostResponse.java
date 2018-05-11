package com.thinkbiganalytics.kylo.model;

import com.thinkbiganalytics.kylo.model.enums.StatementState;

public class StatementsPostResponse {
    private Integer id;
    private String code;
    private StatementState state;
    private StatementOutputResponse output;
    private Double progress;

    // default constructor
    public StatementsPostResponse() {}

    public StatementsPostResponse(Builder sprb ) {
        this.id = sprb.id;
        this.code = sprb.code;
        this.state = sprb.state;
        this.output = sprb.output;
        this.progress = progress;
    }

    public Integer getId() {
        return id;
    }

    public String getCode() {
        return code;
    }

    public StatementState getState() {
        return state;
    }

    public StatementOutputResponse getOutput() {
        return output;
    }

    public Double getProgress() {
        return progress;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StatementsPostResponse{");
        sb.append("id=").append(id);
        sb.append(", code='").append(code).append('\'');
        sb.append(", state='").append(state).append('\'');
        sb.append(", output=").append(output);
        sb.append(", progress=").append(progress);
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private Integer id;
        private String code;
        private StatementState state;
        private StatementOutputResponse output;
        private Double progress;

        public Builder id(Integer id) {
            this.id = id;
            return this;
        }

        public Builder code(String code) {
            this.code = code;
            return this;
        }

        public Builder state(StatementState state) {
            this.state = state;
            return this;
        }

        public Builder output(StatementOutputResponse output) {
            this.output = output;
            return this;
        }

        public Builder progress(Double progress) {
            this.progress = progress;
            return this;
        }
    }


}
