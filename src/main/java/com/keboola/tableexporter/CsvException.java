package com.keboola.tableexporter;

public class CsvException extends Exception {
    public CsvException(String message, Throwable previous) {
        super(message, previous);
    }
}
