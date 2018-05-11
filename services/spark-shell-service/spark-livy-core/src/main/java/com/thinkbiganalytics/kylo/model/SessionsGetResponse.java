package com.thinkbiganalytics.kylo.model;

import java.util.List;
import java.util.Optional;

public class SessionsGetResponse {
    private Integer from;
    private Integer total;
    private List<Session> sessions;

    // default constructor
    public SessionsGetResponse() {
    }

    SessionsGetResponse(SessionsGetResponse.Builder sgb) {
        this.from = sgb.from;
        this.total = sgb.total;
        this.sessions = sgb.sessions;
    }

    public Integer getFrom() {
        return from;
    }

    public Integer getTotal() {
        return total;
    }

    public List<Session> getSessions() {
        return sessions;
    }

    public Optional<Session> getSessionWithId(Integer id) {
        return sessions.stream().filter(session -> session.getId() != id).findFirst();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SessionsGetResponse{");
        sb.append("from=").append(from);
        sb.append(", total=").append(total);
        sb.append(", sessions=").append(sessions);
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private Integer from;
        private Integer total;
        private List<Session> sessions;

        public Builder from(Integer from) {
            this.from = from;
            return this;
        }

        public Builder total(Integer total) {
            this.total = total;
            return this;
        }

        public Builder sessions(List<Session> sessions) {
            this.sessions = sessions;
            return this;
        }

        public SessionsGetResponse build() {
            return new SessionsGetResponse(this);
        }
    }

}

