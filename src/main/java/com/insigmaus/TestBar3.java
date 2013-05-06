package com.insigmaus;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * 
 * @author  Philip Ye [GYe@insigmaus.com]
 * @version V1.0  Create Time: Apr 22, 2013
 */

public class TestBar3 {

    private static final int SBAR_COUNT = 500000;
    // private static final int TICRECORD_COUNT = 50000;// 700000000;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private static final String SBAR_TABLE_NAME = "bar3";

    private static final int TABLE_COUNT = 8;
    private static final String[] TABLE_NAMES = new String[TABLE_COUNT];
    static {
        for (int i = 0; i < TABLE_NAMES.length; i++) {
            String tableName = SBAR_TABLE_NAME + "_" + i;
            TABLE_NAMES[i] = tableName;
        }
    }

    private Configuration conf;

    private static int testCaseToRun = 1;
    /**
     * For test case 1
     */
    private static int yourSelected = 1;
    /**
     * For test case 2, 3
     */
    private static int recordCounter = 0;

    private static void showMenu() {
        System.out.println("\tPlease select test scenario:");
        System.out.println("\t1 = Bar Write data test (5000records)");
        System.out.println("\t2 = Bar Read data test");
        System.out.println("\tothers = Exit");
    }

    public static void main(String[] args) throws Exception {

        showMenu();

        while (true) {
            System.out.print("\n\tyour select is: ");
            int select = System.in.read();
            System.in.skip(2);

            List<Class<? extends Test>> cmds = new ArrayList<Class<? extends Test>>();
            if (select == '1') {
                yourSelected = 1;
                cmds.add(BarSequentialWriteTest.class);

            } else if (select == '2') {
                yourSelected = 2;
                cmds.add(BarFilteredScanTest.class);

            } else {
                return;
            }

            try {
                new TestBar3(HBaseConfiguration.create()).runTests(cmds);
            } catch (Exception e) {
                if (yourSelected == 2) {
                    System.out.println("There is no data in table bar, please select 1 to insert data at first.");
                } else {
                    throw e;
                }
            }

        }

    }

    public TestBar3(Configuration conf) {
        this.conf = conf;
    }

    private void runTests(final List<Class<? extends Test>> cmds) throws Exception {
        // setupTables();

        for (Class<? extends Test> cmd : cmds) {
            if (yourSelected == 1) {
                for(int i = 0; i < TABLE_NAMES.length; i++) {
                    String tableName = TABLE_NAMES[i];
                    if (!tableExists(tableName)) {
                        createTable(tableName);
                    }
                }
            } else if (yourSelected == 2) {
                for (int i = 0; i < TABLE_NAMES.length; i++) {
                    String tableName = TABLE_NAMES[i];
                    if (!tableExists(tableName)) {
                        throw new Exception("The table [" + tableName + "] does not exist");
                    }
                }
            }
            runTestForMultipleRounds(cmd);
        }

        // teardownTables();
    }

    private void setupTables() throws IOException {
        // test case 3 don't need to setup tables
        if (TestBar3.testCaseToRun == 3) {
            return;
        }

        createTable(SBAR_TABLE_NAME);
    }

    public void createTable(String table) throws IOException {
        byte[][] sBarFamilyNames = { FAMILY_NAME };
        createTable(table, sBarFamilyNames);
    }

    private void teardownTables() throws IOException {
        // test case 2 and 3 don't need to tear down tables
        if (TestBar3.testCaseToRun == 2 || TestBar3.testCaseToRun == 3) {
            return;
        }

        dropTable(SBAR_TABLE_NAME);
    }

    public void dropTable(String table) throws IOException {
        deleteTable(table);
    }

    void runTestForMultipleRounds(final Class<? extends Test> cmd) throws IOException {
        System.out.println("****Test Case " + yourSelected + " to run: ");
        double totalStatistic = 0;
        // for (int i = 1; i <= yourSelected; i++) {
        // System.out.println("****Round " + i + "****");
        totalStatistic += runTest(cmd);
        // }

        System.out.println("****Test Case " + yourSelected + " Completed, average:"
                + (totalStatistic) + " MB/s");
        System.out.println("");
    }

    double runTest(final Class<? extends Test> cmd) throws IOException {
        Test t = null;
        try {
            Constructor<? extends Test> constructor = cmd.getDeclaredConstructor(Configuration.class);
            t = constructor.newInstance(this.conf);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Invalid command class: " + cmd.getName()
                    + ".  It does not provide a constructor as described by"
                    + "the javadoc comment.  Available constructors are: "
                    + Arrays.toString(cmd.getConstructors()));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to construct command class", e);
        }
        return t.test();
    }

    private void createTable(String tableName, byte[][] familyNames) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(this.conf);
        {
            boolean tableAvailable = admin.isTableAvailable(tableName);
            if (tableAvailable) {
                deleteTable(tableName);
            }
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for (byte[] familyName : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }

        admin.createTable(tableDescriptor);
        boolean tableAvailable = admin.isTableAvailable(tableName);
        if (tableAvailable) {
            System.out.println("table created:" + tableName);
        }
    }

    private void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(this.conf);

        if (!admin.tableExists(tableName)) {
            return;
        }

        if (!admin.isTableDisabled(tableName)) {
            System.out.println("Disabling table..." + tableName);
            admin.disableTable(tableName);
        }

        System.out.println("Deleting table..." + tableName);
        admin.deleteTable(tableName);
    }

    private boolean tableExists(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(this.conf);

        return (admin.tableExists(tableName));
    }

    /*
     * Format passed long.
     * 
     * @param number
     * 
     * @return Returns zero-prefixed 10-byte wide decimal version of passed
     * number (Does absolute in case number is negative).
     */
    public static byte[] format(final long number, int charCount) {
        byte[] b = new byte[charCount];
        long d = Math.abs(number);
        for (int i = b.length - 1; i >= 0; i--) {
            b[i] = (byte) ((d % 10) + '0');
            d /= 10;
        }
        return b;
    }

    public static byte[] format(final long number) {
        return format(number, 10);
    }

    private static double getMegaBytesPerSec(long durationInMs, long datasize) {
        double megabytesPerSec = (double) datasize * 1000 / durationInMs / 1024 / 1024;
        return megabytesPerSec;
    }

    static class BarTableTest extends Test {

        static final byte[] COLUMN_TIME = Bytes.toBytes("b");
        static final byte[] COLUMN_BAR_LEN = Bytes.toBytes("c");
        static final byte[] COLUMN_FLAGS = Bytes.toBytes("d");
        static final byte[] COLUMN_OPEN = Bytes.toBytes("e");
        static final byte[] COLUMN_HIGH = Bytes.toBytes("f");
        static final byte[] COLUMN_LOW = Bytes.toBytes("g");
        static final byte[] COLUMN_LAST = Bytes.toBytes("h");
        static final byte[] COLUMN_VOLUME = Bytes.toBytes("i");

        static final long timestamp = 1312840920l;

        static final char ROW_KEY_DELIMITER = '_';
        static final char ROW_VALUE_DELIMITER = ',';

        BarTableTest(final Configuration conf, final String testCaseName, final TestOptions options) {
            super(conf, testCaseName, options);
        }

    }


    /*
     * Test Case 1.1
     */
    static class BarSequentialWriteTest extends BarTableTest {

        private DataWriter[] dwList = new DataWriter[TABLE_NAMES.length];

        private Thread[] threadGroup = new Thread[TABLE_NAMES.length];

        private final CountDownLatch latch = new CountDownLatch(TABLE_NAMES.length);

        BarSequentialWriteTest(Configuration conf) {
            super(conf, "[TestCase1.1][sBar SequentialWrite Test]", new TestOptions(0, SBAR_COUNT,
                TABLE_NAMES));

            // Initial threads
            for (int i = 0; i < TABLE_NAMES.length; i++) {
                dwList[i] = new DataWriter(super.startRow, super.totalRows);
                dwList[i].setTaskId(i);
                dwList[i].setLatch(latch);
                threadGroup[i] = new Thread(dwList[i]);
            }
        }

        @Override
        void testThreads() {
            for (int i = 0; i < TABLE_NAMES.length; i++) {
                threadGroup[i].start();
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        class DataWriter implements Runnable {

            private static final int BATCH_SIZE = 60;

            DataWriter(int startRow, int totalRows) {
                this.startRow = startRow;
                this.totalRows = totalRows;
            }

            private int taskId;

            private int startRow;

            private int totalRows;

            private CountDownLatch latch;

            public CountDownLatch getLatch() {
                return latch;
            }

            public void setLatch(CountDownLatch latch) {
                this.latch = latch;
            }

            public int getTaskId() {
                return taskId;
            }

            public void setTaskId(int taskId) {
                this.taskId = taskId;
            }

            public void run() {
                int startRow = this.startRow;
                int lastRow = startRow + this.totalRows;

                for (int i = startRow; i < lastRow; i += BATCH_SIZE) {
                    byte[] key = getRowKey(i);
                    Put put = new Put(key);
                    try {
                        // Writing the data into HBase.
                        putData(put, i);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                    // collectRowStatistics(key.length + 6, put.heapSize());
                }
                this.latch.countDown();
            }

            final private void putData(Put put, int i) throws IOException {
                /*
                 * if (put == null) {
                 * System.err.println("Put object is null!!"); return; }
                 */
                byte[] value = null;

                switch (taskId) {
                    case 0:
                        value = new byte[BATCH_SIZE * 10];
                        for (int k = 0; k < BATCH_SIZE; k++) {
                            byte[] tempValue = format(timestamp + i + k);
                            System.arraycopy(tempValue, 0, value, k * 10, 10);
                        }
                        put.add(FAMILY_NAME, COLUMN_TIME, value);
                        tables[0].put(put);
                        break;
                    case 1:
                        value = new byte[BATCH_SIZE * 4];
                        for (int k = 0; k < BATCH_SIZE; k++) {
                            byte[] tempValue = Bytes.toBytes(1);
                            System.arraycopy(tempValue, 0, value, k * 4, 4);
                        }
                        put.add(FAMILY_NAME, COLUMN_BAR_LEN, value);
                        tables[1].put(put);
                        break;
                    case 2:
                        value = new byte[BATCH_SIZE * 4];
                        for (int k = 0; k < BATCH_SIZE; k++) {
                            byte[] tempValue = Bytes.toBytes(0);
                            System.arraycopy(tempValue, 0, value, k * 4, 4);
                        }
                        put.add(FAMILY_NAME, COLUMN_FLAGS, value);
                        tables[2].put(put);
                        break;
                    case 3:
                        value = new byte[BATCH_SIZE * 8];
                        for (int k = 0; k < BATCH_SIZE; k++) {
                            byte[] tempValue = Bytes.toBytes(RandomUtil.getDouble());
                            System.arraycopy(tempValue, 0, value, k * 8, 8);
                        }
                        put.add(FAMILY_NAME, COLUMN_OPEN, value);
                        tables[3].put(put);
                        break;
                    case 4:
                        value = new byte[BATCH_SIZE * 8];
                        for (int k = 0; k < BATCH_SIZE; k++) {
                            byte[] tempValue = Bytes.toBytes(RandomUtil.getDouble());
                            System.arraycopy(tempValue, 0, value, k * 8, 8);
                        }
                        put.add(FAMILY_NAME, COLUMN_HIGH, value);
                        tables[4].put(put);
                        break;
                    case 5:
                        value = new byte[BATCH_SIZE * 8];
                        for (int k = 0; k < BATCH_SIZE; k++) {
                            byte[] tempValue = Bytes.toBytes(RandomUtil.getDouble());
                            System.arraycopy(tempValue, 0, value, k * 8, 8);
                        }
                        put.add(FAMILY_NAME, COLUMN_LOW, value);
                        tables[5].put(put);
                        break;
                    case 6:
                        value = new byte[BATCH_SIZE * 8];
                        for (int k = 0; k < BATCH_SIZE; k++) {
                            byte[] tempValue = Bytes.toBytes(RandomUtil.getDouble());
                            System.arraycopy(tempValue, 0, value, k * 8, 8);
                        }
                        put.add(FAMILY_NAME, COLUMN_LAST, value);
                        tables[6].put(put);
                        break;
                    case 7:
                        value = new byte[BATCH_SIZE * 4];
                        for (int k = 0; k < BATCH_SIZE; k++) {
                            byte[] tempValue = Bytes.toBytes(RandomUtil.getInt());
                            System.arraycopy(tempValue, 0, value, k * 4, 4);
                        }
                        put.add(FAMILY_NAME, COLUMN_VOLUME, value);
                        tables[7].put(put);
                        break;
                    default:
                        System.err.println("No table can be matched!!");
                }
                put.setWriteToWAL(writeToWAL);
            }
            byte[] getRowKey(final int i) {
                StringBuilder sb = new StringBuilder();
                sb.append(SymbolLoader.randomSymbol());
                sb.append(ROW_KEY_DELIMITER);
                sb.append(timestamp + i);
                return sb.toString().getBytes();
            }
        }
    }

    static class BarFilteredScanTest extends BarTableTest {

        public final static int SCAN_START = 123;
        public final static int SCAN_ROWS = 1200;

        private DataReader[] drList = new DataReader[TABLE_NAMES.length];

        private Thread[] threadGroup = new Thread[TABLE_NAMES.length];

        private final CountDownLatch latch = new CountDownLatch(TABLE_NAMES.length);

        private Set<String> set;

        BarFilteredScanTest(Configuration conf) {
            super(conf, "[TestCase 3][sBar Find By Symbol Test]", new TestOptions(0, SBAR_COUNT,
                TABLE_NAMES));

            set = new HashSet<String>();

            set.add(SymbolLoader.randomSymbol());
            set.add(SymbolLoader.randomSymbol());
            set.add(SymbolLoader.randomSymbol());
            set.add(SymbolLoader.randomSymbol());
            set.add(SymbolLoader.randomSymbol());

            // Initial threads
            for (int i = 0; i < TABLE_NAMES.length; i++) {
                drList[i] = new DataReader(i);
                drList[i].setLatch(latch);
                threadGroup[i] = new Thread(drList[i]);
            }
        }

        class DataReader implements Runnable {

            DataReader(int taskId) {
                this.taskId = taskId;
            }

            private CountDownLatch latch;

            private int taskId;

            public int getTaskId() {
                return taskId;
            }

            public void setTaskId(int taskId) {
                this.taskId = taskId;
            }

            private int counts;

            public CountDownLatch getLatch() {
                return latch;
            }

            public void setLatch(CountDownLatch latch) {
                this.latch = latch;
            }

            public int getCounts() {
                return counts;
            }

            public void setCounts(int counts) {
                this.counts = counts;
            }

            public void run() {
                counts += scaneByPrefixFilter(tables[this.taskId], set);
                latch.countDown();
            }
        }

        void testThreads() {
            System.out.println("Find records of SYMBOL in " + set);
            for (int i = 0; i < TABLE_NAMES.length; i++) {
                threadGroup[i].start();
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            int totalRecords = 0;
            for (int i = 0; i < drList.length; i++) {
                totalRecords += drList[i].getCounts();
            }

            // 60 is the batch size
            System.out.println("Total " + totalRecords * 1 + " records processed.");
        }

        public int scaneByPrefixFilter(HTable table, Set<String> set) {
            ResultScanner rs = null;
            int count = 0;
            try {
                Scan s = new Scan();
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                for (String rowPrifix : set) {
                    filterList.addFilter(new PrefixFilter(rowPrifix.getBytes()));
                }
                s.setFilter(filterList);
                rs = table.getScanner(s);
                for (Result r : rs) {
                    collectRowStatistics(r.getRow().length + 6, r.getWritableSize());
                    ++count;
                }

                // System.out.println("Total " + count + " records processed.");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (rs != null)
                    rs.close();
            }
            return count;

        }
    }

    /**
     * Wraps up options passed to
     * {@link org.apache.hadoop.hbase.PerformanceEvaluation.Test tests}. This
     * makes the reflection logic a little easier to understand...
     */
    static class TestOptions {

        private int startRow;
        private int totalRows;
        private String[] tableNames;

        TestOptions() {}

        TestOptions(int startRow, int totalRows, String[] tableNames) {
            this.startRow = startRow;
            this.totalRows = totalRows;
            this.tableNames = tableNames;

        }

        public int getStartRow() {
            return startRow;
        }

        public int getTotalRows() {
            return totalRows;
        }

        public String[] getTableNames() {
            return tableNames;
        }
    }

    /*
     * A test. Subclass to particularize what happens per row.
     */
    static abstract class Test {

        // Below is make it so when Tests are all running in the one
        // jvm, that they each have a differently seeded Random.
        private static final Random randomSeed = new Random(System.currentTimeMillis());

        private static long nextRandomSeed() {
            return randomSeed.nextLong();
        }

        protected final Random rand = new Random(nextRandomSeed());

        protected final int startRow;
        protected final int totalRows;
        protected String[] tableNames;
        protected HBaseAdmin admin;
        protected HTable[] tables;
        protected volatile Configuration conf;
        protected boolean flushCommits;
        protected boolean writeToWAL;

        // statistics
        /**
         * total size of data, including the overhead (such as column family
         * name, column name, etc)
         */
        private long totalDataSize = 0;

        /**
         * Size of values (size of the original C structure), without the
         * overhead
         */
        private int valueDataSize = 0;

        protected int rowCounter = 0;

        private String testCaseName = "";

        private long startTime = 0;

        /**
         * Note that all subclasses of this class must provide a public
         * constructor that has the exact same list of arguments.
         */
        Test(final Configuration conf, final String testCaseName, final TestOptions options) {
            super();

            this.startRow = options.getStartRow();
            this.totalRows = options.getTotalRows();

            this.tableNames = options.getTableNames();
            this.tables = new HTable[TABLE_NAMES.length];
            this.conf = conf;
            this.flushCommits = true;
            this.writeToWAL = true;

            this.testCaseName = testCaseName;
        }

        private String generateStatus(final int sr, final int i, final int lr) {
            return sr + "/" + i + "/" + lr;
        }

        protected int getReportingPeriod() {
            return this.totalRows;
        }

        void testSetup() throws IOException {
            this.admin = new HBaseAdmin(conf);
            for (int i = 0; i < TABLE_NAMES.length; i++) {
                this.tables[i] = new HTable(conf, Bytes.toBytes(TABLE_NAMES[i]));
                this.tables[i].setAutoFlush(false);
                this.tables[i].setScannerCaching(30);
            }
        }

        void testTakedown() throws IOException {
            if (flushCommits) {
                for (int i = 0; i < TABLE_NAMES.length; i++) {
                    this.tables[i].flushCommits();
                }
                // this.table.flushCommits();
            }
            for (int i = 0; i < TABLE_NAMES.length; i++) {
                this.tables[i].close();
            }
        }

        /*
         * Run test
         * 
         * @return Elapsed time.
         * 
         * @throws IOException
         */
        double test() throws IOException {
            double statisticToReturn = 0;
            long elapsedTime = 0;
            testSetup();

            startTime = System.currentTimeMillis();
            try {
                testThreads();
                elapsedTime = System.currentTimeMillis() - startTime;
            } finally {
                testTakedown();
                statisticToReturn = outputStatistics(elapsedTime);
            }

            return statisticToReturn;
        }

        /*
         * @param i Row index.
         */
        void testThreads() {}

        /**
         * @param elapsedTime
         */
        double outputStatistics(long elapsedTime) {
            double megabytesPerSecOfValue = getMegaBytesPerSec(elapsedTime, valueDataSize);
            double megabytesPerSecOfTotalData = getMegaBytesPerSec(elapsedTime, totalDataSize);

            StringBuilder tableNameList = new StringBuilder();
            if (this.tableNames != null) {
                for (String tableName : this.tableNames) {
                    tableNameList.append(tableName).append(",");
                }
            }
            System.out.println(this.testCaseName + "\n TableNames:" + tableNameList.toString()
                    + ",\n RowCount:" + this.rowCounter + ",\n TimeElapsed:" + elapsedTime
                    + "ms,\n " + megabytesPerSecOfValue + "MB/s(value only, without overhead),\n "
                    + megabytesPerSecOfTotalData + "MB/s(including overhead).");
            System.out.println("");

            return megabytesPerSecOfValue;
        }

        /**
         * Collect statistics for a newly processed row
         * 
         * @param valueSizeForThisRow
         * @param totalSizeForThisRow
         */
        protected void collectRowStatistics(int valueSizeForThisRow, long totalSizeForThisRow) {
            valueDataSize += valueSizeForThisRow;
            totalDataSize += totalSizeForThisRow;
            rowCounter++;

            if (rowCounter > 0 && (rowCounter % getReportingPeriod()) == 0) {
                int lastRow = this.startRow + this.totalRows;
                long elapsedTime = System.currentTimeMillis() - startTime;
                double megabytesPerSecOfValue = getMegaBytesPerSec(elapsedTime, valueDataSize);
                System.out.println(generateStatus(this.startRow, rowCounter, lastRow) + " "
                        + megabytesPerSecOfValue + "MB/s");
            }
        }
    }
}


