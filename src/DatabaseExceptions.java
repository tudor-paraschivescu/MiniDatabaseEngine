/**
 * Class that keeps inner static classes of the Exceptions that could be thrown at runtime while
 * using the database system.
 */
public final class DatabaseExceptions {

    private DatabaseExceptions() {
    }

    /** Runtime exception that will be thrown when null or empty data tries to be inserted. */
    public static class NullOrEmptyDataException extends RuntimeException {
    }

    /** Runtime exception that will be thrown when wrong or invalid data tries to be inserted. */
    public static class InvalidDataException extends RuntimeException {
    }

    /** Runtime exception that will be thrown when an unknown data type tries to be inserted. */
    public static class UnknownDataTypeException extends RuntimeException {
    }

    /** Runtime exception that will be thrown when an invalid data type tries to be inserted. */
    public static class InvalidDataTypeException extends RuntimeException {
    }

    /** Runtime exception that will be thrown when an invalid condition tries to be checked. */
    public static class InvalidConditionException extends RuntimeException {
    }

    /** Runtime exception that will be thrown when an unknown comparator tries to be used. */
    public static class UnknownComparatorException extends RuntimeException {
    }

    /** Runtime exception that will be thrown when an invalid function tries to be used. */
    public static class InvalidFunctionException extends RuntimeException {
    }

    /** Runtime exception that will be thrown when an unknown function tries to be used. */
    public static class UnknownFunctionException extends RuntimeException {
    }
}
