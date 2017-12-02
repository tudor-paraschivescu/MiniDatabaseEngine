import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Class of a database table, that stores the data by columns.
 * Internally it uses hash maps for fast column matching and data type casts.
 */
class Table {

    // Accepted strings for the possible column types
    private static final String COLUMN_TYPE_STRING = "string";
    private static final String COLUMN_TYPE_INTEGER = "int";
    private static final String COLUMN_TYPE_BOOLEAN = "bool";

    /**
     * Enumeration of the accepted data types.
     */
    public enum DataType {
        INTEGER, STRING, BOOLEAN
    }

    /**
     * Enumeration of the accepted aggregation function.
     */
    public enum AggregateFunction {
        MIN, MAX, SUM, AVG, COUNT
    }

    /**
     * Maps the name of a column to the data type of that column.
     */
    private HashMap<String, DataType> dataTypeMap = new HashMap<>();

    /**
     * Maps the name of a column to the corresponding data in the column.
     */
    private LinkedHashMap<String, ArrayList<Object>> columnMap = new LinkedHashMap<>();

    /**
     * The size of the table.
     */
    private int size = 0;

    /**
     * Used to keep all operations on a table synchronized.
     */
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /**
     * Constructor for a database table with the given column names and data types.
     *
     * @param columnNames the names of the columns that will be in the table
     * @param columnTypes the data types of the columns that will be in the table
     */
    Table(final String[] columnNames, final String[] columnTypes) {

        if (columnNames == null || columnTypes == null) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }

        if (columnNames.length != columnTypes.length) {
            throw new DatabaseExceptions.InvalidDataException();
        }

        // Create all the columns and store their data types
        for (int i = 0; i < columnNames.length; i++) {
            switch (columnTypes[i]) {
                case COLUMN_TYPE_STRING:
                    columnMap.put(columnNames[i], new ArrayList<>());
                    dataTypeMap.put(columnNames[i], DataType.STRING);
                    break;
                case COLUMN_TYPE_INTEGER:
                    columnMap.put(columnNames[i], new ArrayList<>());
                    dataTypeMap.put(columnNames[i], DataType.INTEGER);
                    break;
                case COLUMN_TYPE_BOOLEAN:
                    columnMap.put(columnNames[i], new ArrayList<>());
                    dataTypeMap.put(columnNames[i], DataType.BOOLEAN);
                    break;
                default:
                    throw new DatabaseExceptions.UnknownDataTypeException();
            }
        }
    }

    int getSize() {
        return size;
    }

    HashMap<String, DataType> getDataTypeMap() {
        return dataTypeMap;
    }

    LinkedHashMap<String, ArrayList<Object>> getColumnMap() {
        return columnMap;
    }

    /**
     * Lock the table for read operations.
     */
    void lockRead() {
        lock.readLock().lock();
    }

    /**
     * Unlock the table for read operations.
     */
    void unlockRead() {
        lock.readLock().unlock();
    }

    /**
     * Lock the table for write operations.
     */
    void lockWrite() {
        lock.writeLock().lock();
    }

    /**
     * Unlock the table for write operations.
     */
    void unlockWrite() {
        lock.writeLock().unlock();
    }

    /**
     * Append the given data at the end of the table.
     *
     * @param values The values that will be inserted on each column
     */
    void insert(final ArrayList<Object> values) {

        if (values == null) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }

        if (values.size() != columnMap.size()) {
            throw new DatabaseExceptions.InvalidDataException();
        }

        checkDataTypesOfNewRow(values);

        // Iterate through the given array and add the elements at the end of each column
        Iterator<Object> valuesIterator = values.iterator();
        for (Map.Entry<String, ArrayList<Object>> entry : columnMap.entrySet()) {
            ArrayList<Object> column = entry.getValue();
            column.add(valuesIterator.next());
        }

        // Increment table size
        size++;
    }

    void checkParametersSelect(final String[] operations, final String condition) {
        if (operations == null || condition == null || operations.length == 0) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }
    }

    void checkParametersUpdate(final ArrayList<Object> values, final String condition) {
        if (values == null || condition == null || values.size() == 0) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }

        if (values.size() != columnMap.size()) {
            throw new DatabaseExceptions.InvalidDataException();
        }
    }

    ArrayList<ArrayList<Object>> instantiateResultList(final String[] operations) {
        ArrayList<ArrayList<Object>> result = new ArrayList<>();
        for (String ignored : operations) {
            result.add(new ArrayList<>());
        }
        return result;
    }

    void checkDataTypesOfNewRow(final ArrayList<Object> values) {
        // Check if the data type corresponds
        Iterator<Object> valuesIterator = values.iterator();
        for (Map.Entry<String, ArrayList<Object>> entry : columnMap.entrySet()) {
            Object next = valuesIterator.next();
            switch (dataTypeMap.get(entry.getKey())) {
                case INTEGER:
                    if (!(next instanceof Integer)) {
                        throw new DatabaseExceptions.InvalidDataTypeException();
                    }
                    break;
                case STRING:
                    if (!(next instanceof String)) {
                        throw new DatabaseExceptions.InvalidDataTypeException();
                    }
                    break;
                case BOOLEAN:
                    if (!(next instanceof Boolean)) {
                        throw new DatabaseExceptions.InvalidDataTypeException();
                    }
                    break;
                default:
                    throw new DatabaseExceptions.UnknownDataTypeException();
            }
        }
    }
}
