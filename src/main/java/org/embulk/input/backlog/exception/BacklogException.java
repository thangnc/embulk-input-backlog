package org.embulk.input.backlog.exception;

/**
 * @author thangnc
 */
public class BacklogException
        extends Exception {

    private final int statusCode;

    public BacklogException(int statusCode, String message) {
        super(message + ":" + Integer.toString(statusCode));
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
