package com.thinkbiganalytics.kylo.model;

public class SessionsGet {
    private Integer from;
    private Integer size;

    SessionsGet( SessionsGet.Builder sgb ) {
        this.from = sgb.from;
        this.size = sgb.size;
    }

    public Integer getFrom() {
        return from;
    }

    public void setFrom(Integer from) {
        this.from = from;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SessionsGet{");
        sb.append("from=").append(from);
        sb.append(", size=").append(size);
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private Integer from;
        private Integer size;

        public Builder from(Integer from) {
            this.from = from;
            return this;
        }

        public Builder size(Integer size) {
            this.size = size;
            return this;
        }

        public SessionsGet build() {
            return new SessionsGet(this);
        }
    }

}

