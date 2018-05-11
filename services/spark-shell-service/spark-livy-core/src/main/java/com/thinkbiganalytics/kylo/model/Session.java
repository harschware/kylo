package com.thinkbiganalytics.kylo.model;

import com.thinkbiganalytics.kylo.model.enums.SessionKind;
import com.thinkbiganalytics.kylo.model.enums.SessionState;

import java.util.List;
import java.util.Map;

/**
 * @implNote https://livy.incubator.apache.org/docs/latest/rest-api.html#session
 * {"id":0,"appId":null,"owner":null,"proxyUser":null,"state":"starting","kind":"shared",\
 * "appInfo":{"driverLogUrl":null,"sparkUiUrl":null},"log":["stdout: ","\nstderr: "]}
 * ]}
 */
public class Session {
    private Integer id;
    private String appId;
    private String size;
    private String owner;
    private String proxyUser;
    private SessionState state;
    private SessionKind kind;
    private Map<String, String> appInfo;
    private List<String> log;

    public Session() {
    }

    Session(Session.Builder sgb) {
        this.id = sgb.id;
        this.appId = sgb.appId;
        this.size = sgb.size;
        this.owner = sgb.owner;
        this.proxyUser = sgb.proxyUser;
        this.state = sgb.state;
        this.kind = sgb.kind;
        this.appInfo = sgb.appInfo;
        this.log = sgb.log;
    }

    public Integer getId() {
        return id;
    }

    public String getAppId() {
        return appId;
    }

    public String getSize() {
        return size;
    }

    public String getOwner() {
        return owner;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public SessionState getState() {
        return state;
    }

    public SessionKind getKind() {
        return kind;
    }

    public Map<String, String> getAppInfo() {
        return appInfo;
    }

    public List<String> getLog() {
        return log;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Session{");
        sb.append("id=").append(id);
        sb.append(", appId='").append(appId).append('\'');
        sb.append(", size='").append(size).append('\'');
        sb.append(", owner='").append(owner).append('\'');
        sb.append(", proxyUser='").append(proxyUser).append('\'');
        sb.append(", state='").append(state).append('\'');
        sb.append(", kind='").append(kind).append('\'');
        sb.append(", appInfo=").append(appInfo);
        sb.append(", log=").append(log);
        sb.append('}');
        return sb.toString();
    }

    // Only used in testing, this is typically a response object
    public static class Builder {
        private Integer id;
        private String size;
        private String appId;
        private String owner;
        private String proxyUser;
        private SessionState state;
        private SessionKind kind;
        private Map<String, String> appInfo;
        private List<String> log;

        public Builder id(Integer id) {
            this.id = id;
            return this;
        }

        public Builder appId(String appId) {
            this.appId = appId;
            return this;
        }

        public Builder size(String size) {
            this.size = size;
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder proxyUser(String proxyUser) {
            this.proxyUser = proxyUser;
            return this;
        }

        public Builder state(SessionState state) {
            this.state = state;
            return this;
        }

        public Builder kind(SessionKind kind) {
            this.kind = kind;
            return this;
        }

        public Builder appInfo(Map<String, String> appInfo) {
            this.appInfo = appInfo;
            return this;
        }

        public Builder log(List<String> log) {
            this.log = log;
            return this;
        }

        public Session build() {
            return new Session(this);
        }
    }

}

