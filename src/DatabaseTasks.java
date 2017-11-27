import java.util.concurrent.ConcurrentHashMap;

/**
 * Class that keeps inner static classes of the asynchronous tasks needed in the database system.
 */
class DatabaseTasks {

    /** Task that creates a table and adds it to the tables map of the database. */
    static class TableCreationTask implements Runnable {

        private ConcurrentHashMap<String, Table> tableMap;
        private String tableName;
        private String[] columnNames;
        private String[] columnTypes;

        public TableCreationTask(final ConcurrentHashMap<String, Table> tableMap,
                                 final String tableName,
                                 final String[] columnNames,
                                 final String[] columnTypes) {
            this.tableMap = tableMap;
            this.tableName = tableName;
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
        }

        @Override
        public void run() {

        }
    }
}
