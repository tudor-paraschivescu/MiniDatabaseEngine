import java.util.ArrayList;

public class Database implements MyDatabase {
    @Override
    public void initDb(int numWorkerThreads) {

    }

    @Override
    public void stopDb() {

    }

    @Override
    public void createTable(String tableName, String[] columnNames, String[] columnTypes) {

    }

    @Override
    public ArrayList<ArrayList<Object>> select(String tableName, String[] operations, String condition) {
        return null;
    }

    @Override
    public void update(String tableName, ArrayList<Object> values, String condition) {

    }

    @Override
    public void insert(String tableName, ArrayList<Object> values) {

    }

    @Override
    public void startTransaction(String tableName) {

    }

    @Override
    public void endTransaction(String tableName) {

    }
}
