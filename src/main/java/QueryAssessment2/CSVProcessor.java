package QueryAssessment2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Process a CSV file where rows are grouped by non-null values in the first column.
 * Dynamically handles any number of columns.
 */
public class CSVProcessor {

    /**
     * Represents a row in the processed dataframe
     */
    public static class DataRow {
        private Map<String, String> data;

        public DataRow() {
            this.data = new HashMap<>();
        }

        public void put(String column, String value) {
            data.put(column, value);
        }

        public String get(String column) {
            return data.get(column);
        }

        public Map<String, String> getData() {
            return data;
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    /**
     * Represents a dataframe with rows and columns
     */
    public static class DataFrame {
        private List<String> headers;
        private List<DataRow> rows;

        public DataFrame(List<String> headers) {
            this.headers = headers;
            this.rows = new ArrayList<>();
        }

        public void addRow(DataRow row) {
            rows.add(row);
        }

        public List<String> getHeaders() {
            return headers;
        }

        public List<DataRow> getRows() {
            return rows;
        }

        public DataRow getRow(int index) {
            return rows.get(index);
        }

        public Map<String, String> getRowHashMap(int index){
            DataRow rawRow = rows.get(index);

            return rawRow.getData();

        }

        public int size() {
            return rows.size();
        }

        public List<String> getColumnsExceptFirst() {
            return headers.subList(1, headers.size());
        }

        public void printRow(int index) {
            if (index < 0 || index >= rows.size()) {
                System.out.println("Index out of bounds");
                return;
            }
            DataRow row = rows.get(index);
            System.out.println("Row " + index + ":");
            for (String header : headers) {
                System.out.println("  " + header + ": " + row.get(header));
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("DataFrame with ").append(rows.size()).append(" rows\n");
            sb.append("Headers: ").append(headers).append("\n");
            for (int i = 0; i < rows.size(); i++) {
                sb.append("Row ").append(i).append(": ").append(rows.get(i)).append("\n");
            }
            return sb.toString();
        }
    }

    /**
     * Process a CSV file where rows are grouped by non-null values in the first column.
     *
     * @param csvPath Path to the CSV file
     * @return DataFrame with grouped rows
     * @throws IOException If file reading fails
     */
    public static DataFrame processCsvToDataFrame(String csvPath) throws IOException {
        List<String[]> allRows = new ArrayList<>();

        // Read all rows from CSV
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Simple CSV parsing (handles basic cases, not quoted commas)
                String[] values = line.split(",", -1);
                allRows.add(values);
            }
        }

        if (allRows.isEmpty()) {
            throw new IllegalArgumentException("CSV file is empty");
        }

        // Extract headers from the first row
        String[] headers = allRows.get(0);
        int numColumns = headers.length;

        // Create DataFrame
        List<String> headerList = new ArrayList<>();
        for (String header : headers) {
            headerList.add(stripQuotes(header));
        }
        DataFrame df = new DataFrame(headerList);

        // Process rows starting from index 1
        int i = 1;
        while (i < allRows.size()) {
            String[] currentRow = allRows.get(i);

            // Check if this row has a non-null value in the first column
            if (currentRow.length > 0 && isNonNull(currentRow[0])) {
                String firstColumnValue = stripQuotes(currentRow[0]);

                // Initialize lists for each column (excluding the first column)
                List<List<String>> columnParts = new ArrayList<>();
                for (int col = 0; col < numColumns - 1; col++) {
                    columnParts.add(new ArrayList<>());
                }

                // Collect all rows until we hit the next first column value or end of file
                int j = i;
                while (j < allRows.size()) {
                    String[] row = allRows.get(j);

                    // If we hit another first column value (and it's not the first one), stop
                    if (j > i && row.length > 0 && isNonNull(row[0])) {
                        break;
                    }

                    // Add values from all columns (starting from column 1)
                    for (int colIdx = 1; colIdx < Math.min(numColumns, row.length); colIdx++) {
                        if (isNonNull(row[colIdx])) {
                            columnParts.get(colIdx - 1).add(stripQuotes(row[colIdx]));
                        }
                    }

                    j++;
                }

                // Create the row entry
                DataRow dataRow = new DataRow();
                dataRow.put(headerList.get(0), firstColumnValue);

                for (int colIdx = 1; colIdx < numColumns; colIdx++) {
                    String joinedValue = String.join(" ", columnParts.get(colIdx - 1));
                    dataRow.put(headerList.get(colIdx), joinedValue);
                }

                df.addRow(dataRow);

                // Move to the next group
                i = j;
            } else {
                i++;
            }
        }

        return df;
    }

    /**
     * Check if a string value is non-null and not empty
     *
     * @param value The string to check
     * @return true if value is non-null and not empty/whitespace/"<null>"
     */
    private static boolean isNonNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }
        String trimmed = value.trim();
        return !trimmed.equals("<null>");
    }

    /**
     * Strip surrounding quotes from a CSV cell value
     *
     * @param value The cell value
     * @return The value with outer quotes removed if present
     */
    private static String stripQuotes(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    public static HashMap<String, HashMap<String, String>> csv_to_hashmap(String csvPath){
        DataFrame qdf = null;

        try {
            qdf = processCsvToDataFrame(csvPath);
        } catch (IOException e){
            System.err.println("Error reading CSV file: " + e.getMessage());
            e.printStackTrace();
        }

        HashMap<String, HashMap<String, String>> r_val = new HashMap<>();

        for (int i=0; i<qdf.size(); i++) {
            HashMap<String, String> row_map = new HashMap<>();
            String description = null;
            for (Map.Entry<String, String> entry : qdf.getRowHashMap(i).entrySet()) {
                if (entry.getKey().isEmpty())
                    description = entry.getValue();
                else
                    row_map.put(entry.getKey(), entry.getValue());
            }
            r_val.put(description, row_map);
        }

        return r_val;
    }

    /**
     * Example usage
     */
    public static void main(String[] args) {
        try {
            // Test the function
            String csvPath = "/Users/danielarturi/Desktop/COMP 400/JavaWork/Solution3/src/main/java/QueryAssessment2/QueryDoc - Revised_NEO4J.csv";

            DataFrame df = processCsvToDataFrame(csvPath);

            System.out.println("\n--- All rows in DataFrame ---");
            // Loop through and print all rows
            for (int rowIdx = 0; rowIdx < df.size(); rowIdx++) {
                System.out.println("\nRow " + rowIdx + ":");
                DataRow row = df.getRow(rowIdx);
                for (String col : df.getHeaders()) {
                    System.out.println("  " + col + ": " + row.get(col));
                }
            }

        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}