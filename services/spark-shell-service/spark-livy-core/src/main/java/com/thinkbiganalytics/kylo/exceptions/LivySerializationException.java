package com.thinkbiganalytics.kylo.exceptions;

public class LivySerializationException extends LivyException {
    public LivySerializationException() {
        super();
    }

    public LivySerializationException(String s) {
        super(s);
    }

    public LivySerializationException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public LivySerializationException(Throwable cause) {
        super(cause);
    }
}
