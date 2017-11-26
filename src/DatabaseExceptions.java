/**
 * Class that keeps inner static classes of the Exceptions that could be thrown at runtime while
 * using the database system.
 */
class DatabaseExceptions {

    /** Runtime exception that will be thrown when null or empty data tries to be inserted. */
    static class NullOrEmptyDataException extends RuntimeException {
    }

    /** Runtime exception that will be thrown when an unknown data type tries to be inserted. */
    static class UnknownDataTypeException extends RuntimeException {
    }

    /** Runtime exception that will be thrown when wrong or invalid data tries to be inserted. */
    static class InvalidDataException extends RuntimeException {
    }
}
