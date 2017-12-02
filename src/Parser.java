import java.util.HashMap;
import java.util.function.Predicate;

/**
 * Class with static methods that do the parsing of the input.
 */
final class Parser {

    // Constants used to correctly tokenize the condition
    private static final String SEPARATOR_REGEX = " ";
    private static final int CONDITION_TOKENS = 3;

    // Possible comparators
    private static final String CONDITION_COMPARATOR_EQUAL = "==";
    private static final String CONDITION_COMPARATOR_SMALLER = "<";
    private static final String CONDITION_COMPARATOR_BIGGER = ">";

    // The possible values for Boolean DataType
    private static final String TRUE = "true";
    private static final String FALSE = "false";

    // Constants used to correctly tokenize the aggregation function
    private static final String PARENTHESIS_REGEX = "[()]";
    private static final int FUNCTION_TOKENS = 2;

    // Possible functions
    private static final String FUNCTION_MIN = "min";
    private static final String FUNCTION_MAX = "max";
    private static final String FUNCTION_SUM = "sum";
    private static final String FUNCTION_AVG = "avg";
    private static final String FUNCTION_COUNT = "count";

    private Parser() {
    }

    /**
     * Get the name of the column to be checked and the predicate that represents the condition.
     *
     * @param dataTypeMap maps the name of a column to the data type of that column
     * @param condition   not-empty condition in "columnName comparator value" format
     * @return an instance that keeps the name of the column to be checked and the predicate that
     * will be used in checking the condition
     */
    static Database.ColumnNameAndPredicate parseCondition(
            final HashMap<String, Table.DataType> dataTypeMap, final String condition) {

        // Tokenize the condition
        String[] tokens = condition.split(SEPARATOR_REGEX);

        // Check the number of tokens
        if (tokens.length != CONDITION_TOKENS) {
            throw new DatabaseExceptions.InvalidConditionException();
        }

        String columnName = tokens[0];
        String comparator = tokens[1];
        String value = tokens[2];

        // Check for the data types to be the same
        Table.DataType columnDataType = dataTypeMap.get(columnName);
        Table.DataType valueDataType = parseDataType(value);
        if (columnDataType != valueDataType) {
            throw new DatabaseExceptions.InvalidDataTypeException();
        }

        Predicate<Object> func;
        if (comparator.equals(CONDITION_COMPARATOR_EQUAL)) {
            // If the comparator is EQUAL, return functions for each data type
            switch (valueDataType) {
                case INTEGER:
                    func = (Object i) -> (Integer) i == Integer.parseInt(value);
                    break;
                case STRING:
                    func = value::equals;
                    break;
                case BOOLEAN:
                    func = (Object b) -> (Boolean) b == Boolean.parseBoolean(value);
                    break;
                default:
                    throw new DatabaseExceptions.InvalidDataTypeException();
            }
        } else {
            // Return the functions callable only for integer values
            if (valueDataType == Table.DataType.INTEGER) {
                switch (comparator) {
                    case CONDITION_COMPARATOR_SMALLER:
                        func = (Object i) -> (Integer) i < Integer.parseInt(value);
                        break;
                    case CONDITION_COMPARATOR_BIGGER:
                        func = (Object i) -> (Integer) i > Integer.parseInt(value);
                        break;
                    default:
                        throw new DatabaseExceptions.UnknownComparatorException();
                }
            } else {
                throw new DatabaseExceptions.InvalidDataTypeException();
            }
        }

        // Return the name of the column to be checked and the generated predicate
        return new Database.ColumnNameAndPredicate(columnName, func);
    }

    /**
     * Get the name of the column that will be computed and
     * the aggregate function that represents the operation.
     *
     * @param operation operation in "operation(columnName)" format
     * @return an instance that keeps the name of the column to be computed and the type of the
     * aggregation function.
     */
    static Database.ColumnNameAndFunctionType parseAggregationFunction(final String operation) {

        if (operation == null || operation.isEmpty()) {
            throw new DatabaseExceptions.NullOrEmptyDataException();
        }

        // Tokenize the operation and check the number of tokens
        String[] tokens = operation.split(PARENTHESIS_REGEX);
        if (tokens.length != FUNCTION_TOKENS) {
            throw new DatabaseExceptions.InvalidFunctionException();
        }

        String function = tokens[0];
        String columnName = tokens[1];
        Table.AggregateFunction func;

        // Set the aggregate function type accordingly
        switch (function) {
            case FUNCTION_MIN:
                func = Table.AggregateFunction.MIN;
                break;
            case FUNCTION_MAX:
                func = Table.AggregateFunction.MAX;
                break;
            case FUNCTION_SUM:
                func = Table.AggregateFunction.SUM;
                break;
            case FUNCTION_AVG:
                func = Table.AggregateFunction.AVG;
                break;
            case FUNCTION_COUNT:
                func = Table.AggregateFunction.COUNT;
                break;
            default:
                throw new DatabaseExceptions.UnknownFunctionException();
        }

        // Return the name of the column to be computed and the type of the aggregation function
        return new Database.ColumnNameAndFunctionType(columnName, func);
    }

    /**
     * Return tha data type of a given String representation.
     *
     * @param value the string that will be parsed
     * @return the data type of the sequence
     */
    static Table.DataType parseDataType(final String value) {
        if (value.equals(TRUE) || value.equals(FALSE)) {
            return Table.DataType.BOOLEAN;
        } else {
            try {
                Integer.parseInt(value);
                return Table.DataType.INTEGER;
            } catch (NumberFormatException n) {
                return Table.DataType.STRING;
            }
        }
    }

}
