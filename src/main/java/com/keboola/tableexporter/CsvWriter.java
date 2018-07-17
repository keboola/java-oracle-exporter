package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.CsvException;
import com.keboola.tableexporter.exception.UserException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CsvWriter {

    private CSVPrinter csvPrinter;
    private BufferedWriter writer;

    public CsvWriter(String outputFile, String[] header) throws CsvException
    {
        try {
            writer = Files.newBufferedWriter(Paths.get(outputFile));
            if (header != null) {
                csvPrinter = new CSVPrinter(writer, CSVFormat.RFC4180.withHeader(header).withQuoteMode(QuoteMode.ALL));
            } else {
                csvPrinter = new CSVPrinter(writer, CSVFormat.RFC4180);
            }
        } catch (IOException e) {
            throw new CsvException("Failed opening output file.", e);
        }
    }

    public void write(ResultSet content) throws UserException
    {
        try {
            csvPrinter.printRecords(content);
        } catch (SQLException e) {
            throw new UserException("Failed to write csv ", e);
        } catch (IOException e) {
            throw new UserException("Failed to write csv ", e);
        }
    }

    public void close() throws CsvException
    {
        try {
            csvPrinter.flush();
            csvPrinter.close();
            writer.close();
        } catch (IOException e) {
            throw new CsvException("Failed closing file writer", e);
        }
    }
}
