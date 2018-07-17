package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.CsvException;
import com.keboola.tableexporter.exception.UserException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;

public class CsvWriter {

    private CSVPrinter csvPrinter;
    private BufferedWriter writer;

    public CsvWriter(String outputFile, String[] header) throws CsvException {
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

    public void write(ResultSet resultSet, boolean hasLobs) throws UserException, CsvException {
        try {
            if (hasLobs) {
                this.printRecordsWithLobs(resultSet);
            } else {
                csvPrinter.printRecords(resultSet);
            }
        } catch (SQLException e) {
            throw new UserException("Failed to write csv ", e);
        } catch (IOException e) {
            throw new UserException("Failed to write csv ", e);
        }
    }

    public void printRecordsWithLobs(ResultSet resultSet) throws SQLException, IOException, CsvException {
        ResultSetMetaData rsMeta = resultSet.getMetaData();
        int columnCount = rsMeta.getColumnCount();

        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; ++i) {
                if (rsMeta.getColumnTypeName(i) == "CLOB") {
                    Clob clob = resultSet.getClob(i);
                    csvPrinter.print(this.clobToString(clob));
                } else {
                    csvPrinter.print(resultSet.getObject(i));
                }
            }
            csvPrinter.println();
        }
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

    private String clobToString(Clob clob) throws CsvException {
        Reader clobReader = null;
        try {
            clobReader = clob.getCharacterStream();
            final int clobLength = (int) clob.length();
            char[] charBuffer = new char[clobLength];
            int offset = 0;
            int charsRead = clobReader.read(charBuffer, offset, clobLength);
            while (charsRead > 0) {
                offset += charsRead;
                charsRead = clobReader.read(charBuffer, offset, clobLength);
            }
            return new String(charBuffer);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new CsvException("Clob to String conversion error", e);
        } finally {
            if (clobReader != null) {
                try {
                    clobReader.close();
                } catch (IOException e) {
                }
            }
        }
    }
}
