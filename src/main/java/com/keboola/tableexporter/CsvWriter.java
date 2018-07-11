package com.keboola.tableexporter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;

public class CsvWriter {

    private static CSVPrinter csvPrinter;
    private static BufferedWriter writer;

    public CsvWriter(String outputFile, String[] header) throws CsvException
    {
        try {
            writer = Files.newBufferedWriter(Paths.get(outputFile));
            if (header != null) {
                csvPrinter = new CSVPrinter(writer, CSVFormat.RFC4180.withHeader(header));
            } else {
                csvPrinter = new CSVPrinter(writer, CSVFormat.RFC4180);
            }
        } catch (IOException e) {
            throw new CsvException("Failed opening output file.", e);
        }
    }

    public static void writeLine(int lineNum, ResultSet content) throws UserException
    {
        try {
            csvPrinter.printRecord(content);
        } catch (IOException e) {
            throw new UserException("Failed to print row " + lineNum, e);
        }
    }

    public static void flush() throws CsvException
    {
        try {
            csvPrinter.flush();
        } catch (IOException e) {
            throw new CsvException("Failed flushing buffer", e);
        }
    }

    public static void close() throws CsvException
    {
        try {
            csvPrinter.close();
            writer.close();
        } catch (IOException e) {
            throw new CsvException("Failed closing file writer", e);
        }
    }
}
