package com.keboola.tableexporter.exception;

public class CsvException extends Exception {
    public CsvException(String message, Throwable previous) {
        super(message, previous);
    }
}
