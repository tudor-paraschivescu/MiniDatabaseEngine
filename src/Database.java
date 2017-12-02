import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Parallel Database System that implements the most basic commands (select, insert and update).
 * Internally, the parallelism is implemented using an Executor Service with a fixed thread pool.
 */
public final class Database implements MyDatabase {

    /**
     * Maps the name of the tables to the corresponding table.
     * Can be used to create more tables in the same time by different users.
     */
    private ConcurrentHashMap<String, Table> tableMap;

    /**
     * The number of threads that can work in the same time on the database.
     */
    private int numWorkers;

    /**
     * The executor service that manages the database threads.
     */
    private ExecutorService workerService;

    Database() {
        // Create the concurrent hash map that will allow creating multiple tables at a time
        this.tableMap = new ConcurrentHashMap<>();
    }

    @Override
    public void initDb(final int numWorkerThreads) {
        // Instantiate the executor that will schedule the tasks
        numWorkers = numWorkerThreads;
        workerService = Executors.newFixedThreadPool(numWorkerThreads);
    }

    @Override
    public void stopDb() {
        // Close the executor and the running tasks
        workerService.shutdownNow();
    }

    @Override
    public void createTable(final String tableName,
                            final String[] columnNames, final String[] columnTypes) {
        Table newTable = new Table(columnNames, columnTypes);
        if (tableMap.putIfAbsent(tableName, newTable) != null) {
            // A table with the same name already exists
            throw new DatabaseExceptions.InvalidDataException();
        }
    }

    @Override
    public ArrayList<ArrayList<Object>> select(final String tableName, final String[] operations,
                                               final String condition) {
        Table table = tableMap.get(tableName);
        table.lockRead();

        table.checkParametersSelect(operations, condition);
        ArrayList<ArrayList<Object>> result = table.instantiateResultList(operations);

        // Get the indexes that respect the condition
        MyLinkedList<Integer> indexes = getIndexes(table, condition);

        // No table entry met the condition
        if (indexes.size() == 0) {
            table.unlockRead();
            return result;
        }

        for (int i = 0; i < operations.length; i++) {

            String op = operations[i];
            ArrayList<Object> column = table.getColumnMap().get(op);

            if (column != null) {
                // Operation is a column name
                for (Integer index : indexes) {
                    result.get(i).add(column.get(index));
                }
            } else {
                // Operation is an aggregation function
                ColumnNameAndFunctionType colAndFunc = Parser.parseAggregationFunction(op);
                String columnName = colAndFunc.columnName;
                Table.AggregateFunction funcType = colAndFunc.funcType;

                List<List<Object>> partitions = null;
                List<List<Integer>> indexesPartitions = null;

                if (funcType != Table.AggregateFunction.COUNT) {
                    // The function is accepted only on Integer columns
                    if (table.getDataTypeMap().get(columnName) != Table.DataType.INTEGER) {
                        throw new DatabaseExceptions.InvalidDataTypeException();
                    }
                    // Get the column on which the function is being applied on
                    column = table.getColumnMap().get(columnName);

                    // Partition the column so that each worker has an equal part to compute
                    partitions = DatabaseTasks.partitionList(column, numWorkers);
                    indexesPartitions = DatabaseTasks.partitionList(indexes, numWorkers);
                }

                switch (funcType) {
                    case MIN:
                        int min = search(partitions, indexesPartitions,
                                (Integer a, Integer b) -> (a < b) ? a : b);
                        result.get(i).add(min);
                        break;

                    case MAX:
                        int max = search(partitions, indexesPartitions,
                                (Integer a, Integer b) -> (a > b) ? a : b);
                        result.get(i).add(max);
                        break;

                    case SUM:
                        int sum = sum(partitions, indexesPartitions);
                        result.get(i).add(sum);
                        break;

                    case AVG:
                        sum = sum(partitions, indexesPartitions);
                        result.get(i).add((float) sum / indexes.size());
                        break;

                    case COUNT:
                        result.get(i).add(indexes.size());
                        break;

                    default:
                        throw new DatabaseExceptions.UnknownFunctionException();
                }
            }
        }

        table.unlockRead();
        return result;
    }

    @Override
    public void update(final String tableName, final ArrayList<Object> values,
                       final String condition) {

        Table table = tableMap.get(tableName);
        table.lockWrite();

        table.checkParametersUpdate(values, condition);
        table.checkDataTypesOfNewRow(values);

        // Get the indexes that respect the condition
        MyLinkedList<Integer> indexes = getIndexes(table, condition);

        // Iterate through the indexes that respect the condition and update those rows
        for (Integer index : indexes) {
            Iterator<Object> valuesIterator = values.iterator();
            for (Map.Entry<String, ArrayList<Object>> entry : table.getColumnMap().entrySet()) {
                ArrayList<Object> column = entry.getValue();
                column.set(index, valuesIterator.next());
            }
        }

        table.unlockWrite();
    }

    @Override
    public void insert(final String tableName, final ArrayList<Object> values) {
        Table table = tableMap.get(tableName);
        table.lockWrite();
        table.insert(values);
        table.unlockWrite();
    }

    @Override
    public void startTransaction(final String tableName) {
        Table table = tableMap.get(tableName);
        table.lockWrite();
        table.lockRead();
    }

    @Override
    public void endTransaction(final String tableName) {
        Table table = tableMap.get(tableName);
        table.unlockRead();
        table.unlockWrite();
    }

    /**
     * Get the indexes of all the rows of a table where a certain condition is met.
     *
     * @param table     the table where the search is done
     * @param condition the condition
     * @return a list with all the indexes
     */
    private MyLinkedList<Integer> getIndexes(final Table table, final String condition) {
        // The indexes that respect the condition
        MyLinkedList<Integer> indexes = new MyLinkedList<>();

        if (!condition.isEmpty()) {
            // Get the column that must be checked and the condition predicate
            ColumnNameAndPredicate np = Parser.parseCondition(table.getDataTypeMap(), condition);
            String columnName = np.columnName;
            Predicate<Object> predicate = np.conditionPredicate;
            ArrayList<Object> column = table.getColumnMap().get(columnName);

            // Partition the column so that each worker has an equal part to compute
            List<List<Object>> partitions = DatabaseTasks.partitionList(column, numWorkers);
            List<Callable<MyLinkedList<Integer>>> tasks = new ArrayList<>(numWorkers);

            // Create the tasks
            for (List<Object> partition : partitions) {
                tasks.add(new DatabaseTasks.ConditionCheckTask(partition, predicate));
            }

            // Wait for the tasks to end and reunite the lists in the indexes list
            try {
                List<Future<MyLinkedList<Integer>>> futures = workerService.invokeAll(tasks);
                for (Future<MyLinkedList<Integer>> f : futures) {
                    MyLinkedList<Integer> list = f.get();
                    indexes.concat(list);
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }
        } else {
            // The condition is empty
            for (int i = 0; i < table.getSize(); i++) {
                indexes.add(i);
            }
        }

        return indexes;
    }

    /**
     * Search for a certain element in a number of partitions using a given function.
     * The executor service will have numWorkers tasks to execute, one for each partition.
     *
     * @param partitions        the list of partitions
     * @param indexesPartitions the list of indexes
     * @param function          the function that will check
     * @return the searched element
     */
    private int search(final List<List<Object>> partitions,
                       final List<List<Integer>> indexesPartitions,
                       final BiFunction<Integer, Integer, Integer> function) {

        List<Callable<Integer>> tasks = new ArrayList<>(numWorkers);

        // Create the tasks
        for (int i = 0; i < numWorkers; i++) {
            tasks.add(new DatabaseTasks.SearchTask(
                    partitions.get(i), indexesPartitions.get(i), function));
        }

        try {
            int result = 0;

            // Submit the tasks to the executor service and wait for them to end
            List<Future<Integer>> futures = workerService.invokeAll(tasks);

            // Compare the results
            for (Future<Integer> f : futures) {
                int value = f.get();
                result = function.apply(result, value);
            }
            return result;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * Sum all of the elements from a list of partitions.
     *
     * @param partitions       the list of partitions
     * @param indexesPartitions the list of indexes
     * @return the sum of all elements in the list
     */
    private int sum(final List<List<Object>> partitions,
                    final List<List<Integer>> indexesPartitions) {

        List<Callable<Long>> tasks = new ArrayList<>(numWorkers);

        // Create the tasks
        for (int i = 0; i < numWorkers; i++) {
            tasks.add(new DatabaseTasks.SumTask(partitions.get(i), indexesPartitions.get(i)));
        }

        // Wait for the tasks to end and compare the results
        try {
            long sum = 0;

            // Submit the tasks to the executor service and wait for them to end
            List<Future<Long>> futures = workerService.invokeAll(tasks);

            // Sum up the results
            for (Future<Long> f : futures) {
                sum += f.get();
            }

            // In the consistency test, the value is cast to int and an exception would be thrown
            return (int) sum;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * Return type used after parsing a condition to keep both of the column name and predicate.
     */
    static class ColumnNameAndPredicate {
        private String columnName;
        private Predicate<Object> conditionPredicate;

        ColumnNameAndPredicate(final String columnName, final Predicate<Object> func) {
            this.columnName = columnName;
            this.conditionPredicate = func;
        }
    }

    /**
     * Return type used after parsing an aggregation function to keep both of
     * the column name and type of the function.
     */
    static class ColumnNameAndFunctionType {
        private String columnName;
        private Table.AggregateFunction funcType;

        ColumnNameAndFunctionType(final String columnName, final Table.AggregateFunction funcType) {
            this.columnName = columnName;
            this.funcType = funcType;
        }
    }
}
