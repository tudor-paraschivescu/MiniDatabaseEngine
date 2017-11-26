import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Asynchronous Database System that implements the most basic commands (select, insert and update).
 * Internally, the parallelism is implemented using an Executor Service with a fixed thread pool.
 */
public class Database implements MyDatabase {

    /** Map the name of the tables to the corresponding table. */
    private ConcurrentHashMap<String, Table> tableMap;

    /** The number of threads that can work in the same time on the database. */
    private int numWorkers;

    /** The executor service that manages the active threads. */
    private ExecutorService workerService;

    public Database() {
        // Create the concurrent hash map that will allow creating multiple tables at a time
        this.tableMap = new ConcurrentHashMap<>();
    }

    @Override
    public void initDb(final int numWorkerThreads) {
        // Instantiate the executor that will schedule the tasks
        this.numWorkers = numWorkerThreads;
        workerService = Executors.newFixedThreadPool(numWorkerThreads);
    }

    @Override
    public void stopDb() {
        // Close the executor and the running tasks
        workerService.shutdownNow();
    }

    @Override
    public void createTable(final String tableName,
                            final String[] columnNames,
                            final String[] columnTypes) {
        // Submit the task of creating a new table to the database worker service
        workerService.submit(
                new DatabaseTasks.TableCreationTask(tableMap, tableName, columnNames, columnTypes));
    }

    @Override
    public ArrayList<ArrayList<Object>> select(String tableName, String[] operations, String condition) {
        // TODO
        return null;
    }

    @Override
    public void update(String tableName, ArrayList<Object> values, String condition) {
        // TODO
    }

    @Override
    public void insert(String tableName, ArrayList<Object> values) {
        // TODO
    }

    @Override
    public void startTransaction(String tableName) {
        // TODO
    }

    @Override
    public void endTransaction(String tableName) {
        // TODO
    }
}
