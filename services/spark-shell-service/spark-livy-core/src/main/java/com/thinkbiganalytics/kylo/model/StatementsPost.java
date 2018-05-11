package com.thinkbiganalytics.kylo.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StatementsPost {
    private String code;
    private String kind;

    StatementsPost(StatementsPost.Builder sgb ) {
        this.code = sgb.code;
        this.kind = sgb.kind;
    }

    public String getcode() {
        return code;
    }

    public String getkind() {
        return kind;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StatementsPost{");
        sb.append("code='").append(code).append('\'');
        sb.append(", kind='").append(kind).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private String code;
        private String kind;

        public Builder code(String code) {
            this.code = code;
            return this;
        }

        public Builder kind(String kind) {
            this.kind = kind;
            return this;
        }

        public StatementsPost build() {
            return new StatementsPost(this);
        }
    }

}

