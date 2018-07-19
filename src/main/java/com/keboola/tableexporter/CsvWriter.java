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
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class CsvWriter {

    /**
     * Flush the writer after this many records are printed
     */
    private static final int FLUSH_LEVEL = 50000;

    private CSVPrinter csvPrinter;
    private BufferedWriter writer;

    public CsvWriter(String outputFile, String[] header) throws CsvException {
        try {
            writer = Files.newBufferedWriter(Paths.get(outputFile));
            if (header != null) {
                csvPrinter = new CSVPrinter(
                    writer,
                    CSVFormat.RFC4180.withHeader(header).withQuoteMode(QuoteMode.ALL).withRecordSeparator('\n')
                );
            } else {
                csvPrinter = new CSVPrinter(
                    writer,
                    CSVFormat.RFC4180.withQuoteMode(QuoteMode.ALL).withRecordSeparator('\n')
                );
            }
        } catch (IOException e) {
            throw new CsvException("Failed opening output file.", e);
        }
    }

    public int write(ResultSet resultSet, boolean hasLobs) throws UserException, CsvException {
        try {
            if (hasLobs) {
                return this.printRecordsWithLobs(resultSet);
            } else {
                return this.printRecords(resultSet);
            }
        } catch (SQLException e) {
            throw new UserException("Failed to write csv ", e);
        } catch (IOException e) {
            throw new UserException("Failed to write csv ", e);
        }
    }

    public int printRecords(ResultSet resultSet) throws UserException, CsvException {
        try {
            int columnCount = resultSet.getMetaData().getColumnCount();
            int rowCount = 1;
            while(resultSet.next()) {
                for(int i = 1; i <= columnCount; ++i) {
                    csvPrinter.print(resultSet.getObject(i));
                }
                csvPrinter.println();
                rowCount++;
                this.flushIfRequired(rowCount);
            }
            return rowCount;
        } catch (SQLException e) {
            throw new UserException("Failed to write csv ", e);
        } catch (IOException e) {
            throw new UserException("Failed to write csv ", e);
        }
    }

    public int printRecordsWithLobs(ResultSet resultSet) throws SQLException, IOException, CsvException {
        ResultSetMetaData rsMeta = resultSet.getMetaData();
        int columnCount = rsMeta.getColumnCount();
        int rowCount = 1;
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; ++i) {
                if (rsMeta.getColumnTypeName(i) == "CLOB") {
                    Clob clob = resultSet.getClob(i);
                    if (clob == null) {
                        csvPrinter.print("");
                    } else if (clob.length() > Integer.MAX_VALUE) {
                        System.out.println("Clob Column " + rsMeta.getColumnName(i)
                                + " has an entry that is too big for export. It will be truncated.");
                        csvPrinter.print(clob.getSubString(1, Integer.MAX_VALUE));
                    } else {
                        csvPrinter.print(clob.getSubString(1,(int) clob.length()));
                    }
                } else {
                    csvPrinter.print(resultSet.getObject(i));
                }
            }
            csvPrinter.println();
            rowCount++;
            this.flushIfRequired(rowCount);
        }
        return rowCount;
    }

    public void close() throws CsvException {
        try {
            csvPrinter.flush();
            csvPrinter.close();
            writer.close();
        } catch (IOException e) {
            throw new CsvException("Failed closing file writer", e);
        }
    }

    private void flushIfRequired(int rowCount) throws IOException {
        if (rowCount % FLUSH_LEVEL == 0) {
            csvPrinter.flush();
        }
    }
}
