import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * Class of a database table, that stores the data by columns.
 * Internally it uses HashMaps for fast column matching and data type casts.
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
     * Used to keep all operations on a table of blocking type.
     */
    private ReentrantLock lock = new ReentrantLock();

    /**
     * Return type used after parsing a condition to keep both of the column name and predicate.
     */
    public static class ColumnNameAndPredicate {
        private String columnName;
        private Predicate<Object> conditionPredicate;

        ColumnNameAndPredicate(final String columnName, final Predicate<Object> func) {
            this.columnName = columnName;
            this.conditionPredicate = func;
        }
    }

    /**
     * Return type used after parsing an aggregate function to keep both of
     * the column name and type of the function.
     */
    public static class ColumnNameAndFunctionType {
        private String columnName;
        private AggregateFunction funcType;

        ColumnNameAndFunctionType(final String columnName, final AggregateFunction funcType) {
            this.columnName = columnName;
            this.funcType = funcType;
        }
    }

    /**
     * Constructor for a database table with the given column names and data types
     *
     * @param columnNames the names of the columns that will be in the table
     * @param columnTypes the data types of the columns that will be in the table
     * @throws DatabaseExceptions.NullOrEmptyDataException if the parameters are null
     * @throws DatabaseExceptions.InvalidDataException     if the arrays are of different length
     * @throws DatabaseExceptions.UnknownDataTypeException if the columnTypes arrays contains an
     *                                                     unknown type of data
     */
    public Table(final String[] columnNames, final String[] columnTypes) throws
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

    /**
     * Lock the table.
     */
    public void lock() {
        lock.lock();
    }

    /**
     * Unlock the table.
     */
    public void unlock() {
        lock.unlock();
    }

    /**
     * Append the given data at the end of the table
     *
     * @param values The values that will be inserted on each column
     */
    public void insert(final ArrayList<Object> values) {

        if (values == null) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }

        if (values.size() != columnMap.size()) {
            throw new DatabaseExceptions.InvalidDataException();
        }

        // Iterate through the given array and add the elements at the end of each column
        Iterator<Object> valuesIterator = values.iterator();
        for (Map.Entry<String, ArrayList<Object>> entry : columnMap.entrySet()) {
            // TODO: Check if the data type corresponds and then add it
            ArrayList<Object> column = entry.getValue();
            column.add(valuesIterator.next());
        }

        // Increment table size
        size++;
    }

    public ArrayList<ArrayList<Object>> select(final String[] operations, final String condition) {

        if (operations == null || condition == null || operations.length == 0) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }

        // Instantiate the result list
        ArrayList<ArrayList<Object>> result = new ArrayList<>();
        for (int i = 0; i < operations.length; i++) {
            result.add(new ArrayList<>());
        }

        // The indexes that respect the condition
        ArrayList<Integer> indexes = getIndexesFromCondition(condition);

        // No database entry met the condition
        if (indexes.size() == 0) {
            return result;
        }

        for (int i = 0; i < operations.length; i++) {

            String op = operations[i];
            ArrayList<Object> column = columnMap.get(op);

            if (column != null) {
                // Operation is a column name
                // TODO: PARALLELIZE ME
                for (Integer index : indexes) {
                    result.get(i).add(column.get(index));
                }
            } else {
                // Operation is an aggregation function
                ColumnNameAndFunctionType colAndFunc = Parser.parseAggregationFunction(op);
                String columnName = colAndFunc.columnName;
                AggregateFunction funcType = colAndFunc.funcType;

                // TODO: PARALLELIZE ME
                switch (funcType) {
                    case MIN:
                        // The function is accepted only on Integer columns
                        if (dataTypeMap.get(columnName) != DataType.INTEGER) {
                            throw new DatabaseExceptions.InvalidDataTypeException();
                        }

                        column = columnMap.get(columnName);
                        int min = Integer.MAX_VALUE;
                        for (Integer index : indexes) {
                            if (min > (Integer) column.get(index)) {
                                min = (Integer) column.get(index);
                            }
                        }

                        result.get(i).add(min);
                        break;

                    case MAX:
                        // The function is accepted only on Integer columns
                        if (dataTypeMap.get(columnName) != DataType.INTEGER) {
                            throw new DatabaseExceptions.InvalidDataTypeException();
                        }

                        column = columnMap.get(columnName);
                        int max = Integer.MIN_VALUE;
                        for (Integer index : indexes) {
                            if (max < (Integer) column.get(index)) {
                                max = (Integer) column.get(index);
                            }
                        }

                        result.get(i).add(max);
                        break;

                    case SUM:
                        // The function is accepted only on Integer columns
                        if (dataTypeMap.get(columnName) != DataType.INTEGER) {
                            throw new DatabaseExceptions.InvalidDataTypeException();
                        }

                        column = columnMap.get(columnName);
                        int sum = 0;
                        for (Integer index : indexes) {
                            sum += (Integer) column.get(index);
                        }

                        result.get(i).add(sum);
                        break;

                    case AVG:
                        // The function is accepted only on Integer columns
                        if (dataTypeMap.get(columnName) != DataType.INTEGER) {
                            throw new DatabaseExceptions.InvalidDataTypeException();
                        }

                        column = columnMap.get(columnName);
                        int avg = 0;
                        for (Integer index : indexes) {
                            avg += (Integer) column.get(index);
                        }

                        result.get(i).add(avg / indexes.size());
                        break;

                    case COUNT:
                        result.get(i).add(indexes.size());
                        break;

                    default:
                        throw new DatabaseExceptions.UnknownFunctionException();
                }
            }
        }

        return result;
    }

    public void update(final ArrayList<Object> values, final String condition) {
        if (values == null || condition == null || values.size() == 0) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }

        if (values.size() != columnMap.size()) {
            throw new DatabaseExceptions.InvalidDataException();
        }

        // The indexes that respect the condition
        ArrayList<Integer> indexes = getIndexesFromCondition(condition);

        for (Integer index : indexes) {
            Iterator<Object> valuesIterator = values.iterator();
            for (Map.Entry<String, ArrayList<Object>> entry : columnMap.entrySet()) {
                // TODO: Check if the data type corresponds and then add it
                ArrayList<Object> column = entry.getValue();
                column.set(index, valuesIterator.next());
            }
        }
    }

    private ArrayList<Integer> getIndexesFromCondition(final String condition) {
        // The indexes that respect the condition
        ArrayList<Integer> indexes = new ArrayList<>();

        if (!condition.isEmpty()) {
            // Get the column that must be checked and the condition predicate
            ColumnNameAndPredicate nameAndPredicate = Parser.parseCondition(dataTypeMap, condition);
            String columnName = nameAndPredicate.columnName;
            Predicate<Object> conditionPredicate = nameAndPredicate.conditionPredicate;

            // Add all the indexes where the condition is respected
            ArrayList<Object> column = columnMap.get(columnName);
            for (int i = 0; i < column.size(); i++) {
                if (conditionPredicate.test(column.get(i))) {
                    indexes.add(i);
                }
            }
        } else {
            // The condition is empty
            for (int i = 0; i < size; i++) {
                indexes.add(i);
            }
        }

        return indexes;
    }
}
