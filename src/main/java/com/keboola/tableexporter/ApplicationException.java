package com.keboola.tableexporter;

public class ApplicationException extends Exception {
    public ApplicationException(String message, Throwable previous) {
        super(message, previous);
    }
}
