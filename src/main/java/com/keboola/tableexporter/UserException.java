package com.keboola.tableexporter;

public class UserException extends Exception {
    public UserException(String message, Throwable previous) {
        super(message, previous);
    }   
}
