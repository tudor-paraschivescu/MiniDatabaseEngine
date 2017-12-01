import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Class that keeps inner static classes of the asynchronous tasks needed in the database system.
 */
public final class DatabaseTasks {

    /** Enumerates the possible tasks that could be created. */
    public enum TaskType {
        CONDITION_CHECK, SELECT_COLUMN
    }

    private DatabaseTasks() {
    }

    public static <T> List<List<T>> partitionList(final List<T> list, final int numThreads) {

        List<List<T>> partitionLists = new ArrayList<>(numThreads);
        int partitionSize = list.size() / numThreads;

        // Initialize first partitions - 1 sub lists
        for (int i = 0; i < numThreads - 1; i++) {
            partitionLists.add(list.subList(i * partitionSize, (i + 1) * partitionSize));
        }

        // Initialize last sub list (which could have a bigger partition)
        partitionLists.add(list.subList((numThreads - 1) * partitionSize, list.size()));

        return partitionLists;
    }

    /**
     * Task that checks a condition from a query and
     * returns the indexes where the condition is met.
     */
    public static class ConditionCheckTask implements Callable<List<Integer>> {

        /** The list that will be checked against the condition. */
        private List<Object> listToCheck;

        /** The condition that must be checked for an index to be added. */
        private Predicate<Object> predicate;

        public ConditionCheckTask(final List<Object> list, final Predicate<Object> condition) {
            this.listToCheck = list;
            this.predicate = condition;
        }

        @Override
        public List<Integer> call() {
            List<Integer> indexes = new ArrayList<>();

            for (int i = 0; i < listToCheck.size(); i++) {
                if (predicate.test(listToCheck.get(i))) {
                    indexes.add(i);
                }
            }

            return indexes;
        }
    }

    /**
     * Task that searches for an element in a list at given indexes.
     */
    public static class SearchTask implements Callable<Integer> {

        /** The list that will be searched. */
        private List<Object> listToSearch;

        /** The list that keeps the indexes. */
        private List<Integer> indexes;

        /** The function that must be checked for the element to be changed. */
        private BiFunction<Integer, Integer, Integer> function;

        public SearchTask(final List<Object> list, final List<Integer> indexes,
                          final BiFunction<Integer, Integer, Integer> function) {
            this.listToSearch = list;
            this.indexes = indexes;
            this.function = function;
        }

        @Override
        public Integer call() {
            int value = (int) listToSearch.get(indexes.get(0));
            for (Integer idx : indexes) {
                value = function.apply(value, (int) listToSearch.get(idx));
            }
            return value;
        }
    }

    /**
     * Task that makes the sum of the element in the list at given indexes.
     * The call() method returns a long to try to avoid overflow.
     */
    public static class SumTask implements Callable<Long> {

        /** The list that will be summed. */
        private List<Object> listToSum;

        /** The list that keeps the indexes. */
        private List<Integer> indexes;

        public SumTask(final List<Object> list, final List<Integer> indexes) {
            this.listToSum = list;
            this.indexes = indexes;
        }

        @Override
        public Long call() {
            long sum = 0;
            for (Integer idx : indexes) {
                sum += (int) listToSum.get(idx);
            }
            return sum;
        }
    }
}
