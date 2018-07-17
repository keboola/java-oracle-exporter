package com.keboola.tableexporter.exception;

public class ApplicationException extends Exception {
    public ApplicationException(String message, Throwable previous) {
        super(message, previous);
    }
}
