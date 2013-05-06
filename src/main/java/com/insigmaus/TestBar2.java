package com.insigmaus;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

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

public class TestBar2 {

    private static final int SBAR_COUNT = 500000;
    // private static final int TICRECORD_COUNT = 50000;// 700000000;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private static final String SBAR_TABLE_NAME = "bar2";

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
                // Configuration myConf = HBaseConfiguration.create();
                // myConf.set("hbase.zookeeper.quorum", "10.0.37.20");
                // myConf.set("hbase.zookeeper.property.clientPort", "2181");
                // myConf.set("hbase.master", "10.0.37.20:59787");

                new TestBar2(HBaseConfiguration.create()).runTests(cmds);
                // new TestBar2(myConf).runTests(cmds);
            } catch (Exception e) {
            	if (yourSelected == 2) {
            		System.out.println("There is no data in table bar, please select 1 to insert data at first.");
            	} else {
            		throw e;
            	}
            }

        }

    }

    public TestBar2(Configuration conf) {
        this.conf = conf;
    }

    private void runTests(final List<Class<? extends Test>> cmds) throws Exception {
        //setupTables();

        for (Class<? extends Test> cmd : cmds) {
        	if (yourSelected == 1) {
        		if (!tableExists(SBAR_TABLE_NAME)) {
        			createTable(SBAR_TABLE_NAME);
        		}
        	} else if (yourSelected == 2) {
        		if (!tableExists(SBAR_TABLE_NAME)) {
        			throw new Exception("no table");
        		}
        	}
            runTestForMultipleRounds(cmd);
        }

        //teardownTables();
    }

    private void setupTables() throws IOException {
        // test case 3 don't need to setup tables
        if (TestBar2.testCaseToRun == 3) {
            return;
        }

        createTable(SBAR_TABLE_NAME);

        /*
         * byte[][] sTicRecordFamilyNames = { FAMILY_NAME, FAMILY_NAME };
         * createTable(STIK_RECORD_TABLE_NAME, sTicRecordFamilyNames);
         */
    }

    public void createTable(String table) throws IOException {
        byte[][] sBarFamilyNames = { FAMILY_NAME };
        createTable(table, sBarFamilyNames);
    }

    private void teardownTables() throws IOException {
        // test case 2 and 3 don't need to tear down tables
        if (TestBar2.testCaseToRun == 2 || TestBar2.testCaseToRun == 3) {
            return;
        }

        dropTable(SBAR_TABLE_NAME);
        /*
         * deleteTable(STIK_RECORD_TABLE_NAME);
         */
    }

    public void dropTable(String table) throws IOException {
        deleteTable(table);
    }

    void runTestForMultipleRounds(final Class<? extends Test> cmd) throws IOException {
        System.out.println("****Test Case " + yourSelected + " to run: ");
        double totalStatistic = 0;
        //for (int i = 1; i <= yourSelected; i++) {
        //    System.out.println("****Round " + i + "****");
            totalStatistic += runTest(cmd);
        //}

        System.out.println("****Test Case " + yourSelected + " Completed, average:"
                + (totalStatistic ) + " MB/s");
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
        // Configuration conf = HBaseConfiguration.create();
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
        // Configuration conf = HBaseConfiguration.create();
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

        BarSequentialWriteTest(Configuration conf) {
            super(conf, "[TestCase1.1][sBar SequentialWrite Test]", new TestOptions(0, SBAR_COUNT,
                SBAR_TABLE_NAME));
            
        }

        @Override
        void testRow(final int i) throws IOException {
            byte[] key = getRowKey(i);
            Put put = new Put(key);

            byte[] value = getRowValue(i);

            put.add(FAMILY_NAME, COLUMN_TIME, format(timestamp + i));
            put.add(FAMILY_NAME, COLUMN_BAR_LEN, Bytes.toBytes(1));
            put.add(FAMILY_NAME, COLUMN_FLAGS, Bytes.toBytes(0));
            put.add(FAMILY_NAME, COLUMN_OPEN, Bytes.toBytes(RandomUtil.getDouble()));
            put.add(FAMILY_NAME, COLUMN_HIGH, Bytes.toBytes(RandomUtil.getDouble()));
            put.add(FAMILY_NAME, COLUMN_LOW, Bytes.toBytes(RandomUtil.getDouble()));
            put.add(FAMILY_NAME, COLUMN_LAST, Bytes.toBytes(RandomUtil.getDouble()));
            put.add(FAMILY_NAME, COLUMN_VOLUME, Bytes.toBytes(RandomUtil.getInt()));

            put.setWriteToWAL(writeToWAL);

            collectRowStatistics(key.length + 8*5 + 4*3, put.heapSize());

            table.put(put);

        }

        byte[] getRowKey(final int i) {
            StringBuilder sb = new StringBuilder();
            sb.append(SymbolLoader.randomSymbol());
            sb.append(ROW_KEY_DELIMITER);
            sb.append(timestamp + i);
            return sb.toString().getBytes();
        }
        
        byte[] getTime(final int i) {
        	return format(timestamp + i);
        }

        byte[] getRowValue(final int i) {
            StringBuilder sb = new StringBuilder();
            sb.append(1) // iBarLength
              .append(ROW_VALUE_DELIMITER).append(0) // uFlags
              .append(ROW_VALUE_DELIMITER).append(RandomUtil.getDouble()) // dOpen
              .append(ROW_VALUE_DELIMITER).append(RandomUtil.getDouble()) // dHigh
              .append(ROW_VALUE_DELIMITER).append(RandomUtil.getDouble()) // dLow
              .append(ROW_VALUE_DELIMITER).append(RandomUtil.getDouble()) // dLast
              .append(ROW_VALUE_DELIMITER).append(RandomUtil.getInt()) // iVolume
            ;
            return sb.toString().getBytes();
        }

    }

    static class BarFilteredScanTest extends BarTableTest {

        public final static int SCAN_START = 123;
        public final static int SCAN_ROWS = 1200;

        BarFilteredScanTest(Configuration conf) {
            super(
                  conf,
                  "[TestCase 2][sBar Find By Symbol Test]",
                  new TestOptions(0, SBAR_COUNT, SBAR_TABLE_NAME));
        }

        @Override
        void testTimed() throws IOException {
        	Set<String> set = new HashSet<String>();
        	
        	set.add(SymbolLoader.randomSymbol());
        	set.add(SymbolLoader.randomSymbol());
        	set.add(SymbolLoader.randomSymbol());
        	set.add(SymbolLoader.randomSymbol());
        	set.add(SymbolLoader.randomSymbol());
        	
        	System.out.println("Find records of SYMBOL in " + set);
        	scaneByPrefixFilter(table, set);
        }

        public void scaneByPrefixFilter(HTable table, Set<String> set) {
            ResultScanner rs = null;
            try {
                Scan s = new Scan();
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                for (String rowPrifix : set) {
                    filterList.addFilter(new PrefixFilter(rowPrifix.getBytes()));
                }
                s.setFilter(filterList);
                rs = table.getScanner(s);
                int count = 0;
                for (Result r : rs) {
                    collectRowStatistics(r.getRow().length + 8*5 + 4*3,
                                         r.getWritableSize());
                    ++count;
                }

                System.out.println("Total " + count + " records processed.");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (rs != null)
                    rs.close();
            }

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
        private String tableName;

        TestOptions() {}

        TestOptions(int startRow, int totalRows, String tableName) {
            this.startRow = startRow;
            this.totalRows = totalRows;
            this.tableName = tableName;

        }

        public int getStartRow() {
            return startRow;
        }

        public int getTotalRows() {
            return totalRows;
        }

        public String getTableName() {
            return tableName;
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
        protected String tableName;
        protected HBaseAdmin admin;
        protected HTable table;
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

            this.tableName = options.getTableName();
            this.table = null;
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
            this.table = new HTable(conf, Bytes.toBytes(tableName));
            this.table.setAutoFlush(false);
            this.table.setScannerCaching(30);
        }

        void testTakedown() throws IOException {
            if (flushCommits) {
                this.table.flushCommits();
            }
            table.close();
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
                testTimed();
                elapsedTime = System.currentTimeMillis() - startTime;
            } finally {
                testTakedown();

                statisticToReturn = outputStatistics(elapsedTime);
            }

            return statisticToReturn;
        }

        /**
         * Provides an extension point for tests that don't want a per row
         * invocation.
         */
        void testTimed() throws IOException {
            int lastRow = this.startRow + this.totalRows;

            for (int i = this.startRow; i < lastRow; i++) {
                testRow(i);
            }
        }

        /*
         * Test for individual row.
         * 
         * @param i Row index.
         */
        void testRow(final int i) throws IOException {}

        /**
         * @param elapsedTime
         */
        double outputStatistics(long elapsedTime) {
            double megabytesPerSecOfValue = getMegaBytesPerSec(elapsedTime, valueDataSize);
            double megabytesPerSecOfTotalData = getMegaBytesPerSec(elapsedTime, totalDataSize);

            System.out.println(this.testCaseName + "\n TableName:" + this.tableName
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
