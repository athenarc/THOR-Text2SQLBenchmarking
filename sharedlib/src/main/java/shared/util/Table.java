package shared.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class represents a table. It is used to save the statistics of the
 * components.
 */
public class Table {

    /**
     * This class represents a {@link Table} row. A {@link Table} can have a list of
     * rows.
     */
    public static class Row {

        List<String> columnValues; // The values of the row.

        /**
         * Constructor.
         */
        public Row(List<String> values) {
            this.columnValues = values;
        }

        public Row(String... values) {
            this.columnValues = new ArrayList<>();
            for (String s : values)
                this.columnValues.add(s);
        }

        public void add(String value) {
            this.columnValues.add(value);
        }

        /**
         * @return The values of the row.
         */
        public List<String> getValues() {
            return columnValues;
        }

    }

    private String title; // The title of the table.
    private List<String> columnTitles; // The titles of the table's columns.
    private List<Row> rows; // The rows of the table.

    /**
     * Use this constructor in the case where each row contains 2 values only. The
     * column titles will be initialized to the default values "Description",
     * "Value". The table title will be left empty.
     */
    public Table(List<Row> rows) {
        this.title = null;
        this.columnTitles = new ArrayList<>(Arrays.asList("Description", "Value"));
        this.rows = rows;
    }

    /**
     * Use this constructor in the case where each row contains 2 values only. The
     * column titles will be initialized to the default values "Description",
     * "Value". The table title will be set to the value of the first argument.
     */
    public Table(String title, List<Row> rows) {
        this.title = title;
        this.columnTitles = new ArrayList<>(Arrays.asList("Description", "Value"));
        this.rows = rows;
    }

    /**
     * Use this constructor to create a table with rows of any size. Make sure that
     * the length of the columnTitles argument is equal to the length of each row.
     * The table title will be left empty.
     */
    public Table(List<String> columnTitles, List<Row> rows) {
        this.title = null;
        this.columnTitles = columnTitles;
        this.rows = rows;
    }

    /**
     * Use this constructor to create a table with rows of any size. Make sure that
     * the length of the columnTitles argument is equal to the length of each row.
     * The table title will be set to the value of the first argument.
     */
    public Table(String title, List<String> columnTitles, List<Row> rows) {
        this.title = title;
        this.columnTitles = columnTitles;
        this.rows = rows;
    }

    /**
     * Load directly from an sql {@link ResultSet}
     * 
     * @param resultSet An sql result set
     * @throws SQLException
     */
    public Table(ResultSet rs) throws SQLException {

        // Get the metaData of the result set
        ResultSetMetaData metaData = rs.getMetaData();

        // fill names of columns
        this.columnTitles = new ArrayList<>();
        int columnCount = metaData.getColumnCount();
        for (int column = 1; column <= columnCount; column++)
            this.columnTitles.add(metaData.getColumnName(column));
        

        // data of the table
        this.rows = new ArrayList<>();
        while (rs.next()) {
            Row row = new Row();
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++)
                row.add( (rs.getObject(columnIndex) != null) ? rs.getObject(columnIndex).toString() : "null" );            
            rows.add(row);
        }
    }

    /**
     * @return The title of the table.
     */
    public String getTitle() {
        return title;
    }

    /**
     * @return The titles of the columns.
     */
    public List<String> getColumnTitles() {
        return columnTitles;
    }

    /**
     * @return The rows of the table.
     */
    public List<Row> getRows() {
        return rows;
    }
    
}