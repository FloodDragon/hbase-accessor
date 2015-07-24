package com.xunge.persistence.hbase.exc;

public class TransactionException extends Exception {

    private static final long serialVersionUID = 7273525983622126275L;

    public TransactionException(String reason) {
        super(reason);
    }

    public TransactionException(String reason, Exception e) {
        super(reason, e);
    }

    public TransactionException() {
        super();
    }

    public TransactionException(String message, Throwable cause,
                                boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        // TODO Auto-generated constructor stub
    }

    public TransactionException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

    public TransactionException(Throwable cause) {
        super(cause);
    }
}
