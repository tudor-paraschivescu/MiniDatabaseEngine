import java.util.ArrayList;
import java.util.HashMap;

/**
 * Class of a database table, that stores the data by columns.
 * Internally it uses HashMaps for fast column matching and data type casts.
 */
class Table {

    // Accepted strings for the possible column types
    private static final String COLUMN_TYPE_STRING = "string";
    private static final String COLUMN_TYPE_INTEGER = "int";
    private static final String COLUMN_TYPE_BOOLEAN = "bool";

    /** Enumeration of the accepted data types */
    private enum DataType { INTEGER, STRING, BOOLEAN }

    /** Map the name of a column to the data type of that column */
    private HashMap<String, DataType> dataTypeMap = new HashMap<>();

    /** Map the name of a column to the corresponding data in the column */
    private HashMap<String, ArrayList<?>> columnMap = new HashMap<>();

    /**
     * Constructor for a database table with the given column names and data types
     * @param columnNames the names of the columns that will be in the table
     * @param columnTypes the data types of the columns that will be in the table
     * @throws DatabaseExceptions.NullOrEmptyDataException if the parameters are null
     * @throws DatabaseExceptions.InvalidDataException if the arrays are of different length
     * @throws DatabaseExceptions.UnknownDataTypeException if the columnTypes arrays contains an
     * unknown type of data
     */
    Table(final String[] columnNames, final String[] columnTypes) throws
            DatabaseExceptions.NullOrEmptyDataException,
            DatabaseExceptions.InvalidDataException,
            DatabaseExceptions.UnknownDataTypeException {

        if (columnNames == null || columnTypes == null) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }

        if (columnNames.length != columnTypes.length) {
            throw new DatabaseExceptions.InvalidDataException();
        }

        for (int i = 0; i < columnNames.length; i++) {
            switch (columnTypes[i]) {
                case COLUMN_TYPE_STRING:
                    columnMap.put(columnNames[i], new ArrayList<String>());
                    dataTypeMap.put(columnNames[i], DataType.STRING);
                    break;
                case COLUMN_TYPE_INTEGER:
                    columnMap.put(columnNames[i], new ArrayList<Integer>());
                    dataTypeMap.put(columnNames[i], DataType.INTEGER);
                    break;
                case COLUMN_TYPE_BOOLEAN:
                    columnMap.put(columnNames[i], new ArrayList<Boolean>());
                    dataTypeMap.put(columnNames[i], DataType.BOOLEAN);
                    break;
                default:
                    throw new DatabaseExceptions.UnknownDataTypeException();
            }
        }
    }

    ArrayList<ArrayList<Object>> select(String[] operations, String condition) {

        if (operations == null || condition == null || operations.length == 0) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }

        ArrayList<ArrayList<Object>> result = new ArrayList<>();

        return result;
    }
}
