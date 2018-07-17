package com.keboola.tableexporter.exception;

public class UserException extends Exception {
    public UserException(String message, Throwable previous) {
        super(message, previous);
    }   
}
