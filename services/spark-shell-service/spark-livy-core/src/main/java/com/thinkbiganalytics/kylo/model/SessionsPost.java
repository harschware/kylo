package com.thinkbiganalytics.kylo.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * @implNote https://livy.incubator.apache.org/docs/latest/rest-api.html
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SessionsPost {
    private String kind;
    private String user;
    private Map<String,String> conf;
    private List<String> jars;

    SessionsPost(SessionsPost.Builder sgb ) {
        this.kind = sgb.kind;
        this.user = sgb.user;
        this.conf = sgb.conf;
        this.jars = sgb.jars;
    }

    public String getKind() {
        return kind;
    }

    public String getUser() {
        return user;
    }

    public Map<String, String> getConf() {
        return conf;
    }

    public List<String> getJars() {
        return jars;
    }

    @Override
    public String toString() {
        return "SessionsPost{" +
                "kind='" + kind + '\'' +
                ", user='" + user + '\'' +
                ", conf=" + conf +
                '}';
    }

    public static class Builder {
        private String kind;
        private String user;
        private Map<String,String> conf;
        private List<String> jars;

        public Builder kind(String kind) {
            this.kind = kind;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder conf(Map<String, String> conf) {
            this.conf = conf;
            return this;
        }

        public Builder jars(List<String> jars) {
            this.jars = jars;
            return this;
        }

        public SessionsPost build() {
            return new SessionsPost(this);
        }
    }

}

