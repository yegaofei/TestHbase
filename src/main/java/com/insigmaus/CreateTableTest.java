package com.insigmaus;


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class CreateTableTest {

    private static final int SBAR_COUNT = 5000;
    // private static final int TICRECORD_COUNT = 50000;// 700000000;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("d"); // prefer short
                                                                 // family
                                                                 // name
    public static final byte[] FAMILY_NAME2 = Bytes.toBytes("f");

    private static final String SBAR_TABLE_NAME = "sBar";
    private static final String STIK_RECORD_TABLE_NAME = "sTicRecord";

    private Configuration conf;

    private static int testCaseToRun = 1;
    /**
     * For test case 1
     */
    private static int testCaseRunCounter = 1;
    /**
     * For test case 2, 3
     */
    private static int recordCounter = 0;

    public static void main(String[] args) throws Exception {
        List<Class<? extends Test>> cmds = new ArrayList<Class<? extends Test>>();
        if (args.length >= 1) {
            testCaseToRun = Integer.parseInt(args[0]);
            if (testCaseToRun == 1) {
                cmds.add(BarSequentialWriteTest.class);
                cmds.add(BarFilteredScanTest.class);
                cmds.add(BarRandomScanTest.class);

                if (args.length >= 2) {
                    testCaseRunCounter = Integer.parseInt(args[1]);
                }
            } else if (testCaseToRun == 2) {
                cmds.add(TicQuoteSequentialWriteTest.class);
            } else if (testCaseToRun == 3) {
                cmds.add(TicQuoteSequentialReadTest.class);
            }

            if (testCaseToRun == 2 || testCaseToRun == 3) {
                int counterInMillion = 1;
                if (args.length >= 2) {
                    counterInMillion = Integer.parseInt(args[1]);
                }
                recordCounter = 1000000 * counterInMillion;
            }
        }

        new CreateTableTest(HBaseConfiguration.create()).runTests(cmds);

    }

    public CreateTableTest(Configuration conf) {
        this.conf = conf;
    }

    private void runTests(final List<Class<? extends Test>> cmds) throws IOException {
        setupTables();

        for (Class<? extends Test> cmd : cmds) {
            runTestForMultipleRounds(cmd);
        }

        teardownTables();
    }

    private void setupTables() throws IOException {
        // test case 3 don't need to setup tables
        if (CreateTableTest.testCaseToRun == 3) {
            return;
        }
        byte[][] sBarFamilyNames = { FAMILY_NAME };
        createTable(SBAR_TABLE_NAME, sBarFamilyNames);

        byte[][] sTicRecordFamilyNames = { FAMILY_NAME, FAMILY_NAME2 };
        createTable(STIK_RECORD_TABLE_NAME, sTicRecordFamilyNames);
    }

    private void teardownTables() throws IOException {
        // test case 2 and 3 don't need to tear down tables
        if (CreateTableTest.testCaseToRun == 2 || CreateTableTest.testCaseToRun == 3) {
            return;
        }
        deleteTable(SBAR_TABLE_NAME);
        deleteTable(STIK_RECORD_TABLE_NAME);
    }

    void runTestForMultipleRounds(final Class<? extends Test> cmd) throws IOException {
        System.out.println("****Test Case " + cmd.getName() + " Rounds to run: "
                + testCaseRunCounter);
        double totalStatistic = 0;
        for (int i = 1; i <= testCaseRunCounter; i++) {
            System.out.println("****Round " + i + "****");
            totalStatistic += runTest(cmd);
        }

        System.out.println("****Test Case " + cmd.getName() + " Completed, average:"
                + (totalStatistic / testCaseRunCounter));
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

        if (!admin.isTableDisabled(tableName)) {
            System.out.println("Disabling table..." + tableName);
            admin.disableTable(tableName);
        }

        System.out.println("Deleting table..." + tableName);
        admin.deleteTable(tableName);
    }

    /*
     * Format passed integer.
     * 
     * @param number
     * 
     * @return Returns zero-prefixed 10-byte wide decimal version of passed
     * number (Does absolute in case number is negative).
     */
    public static byte[] format(final int number, int charCount) {
        byte[] b = new byte[charCount];
        int d = Math.abs(number);
        for (int i = b.length - 1; i >= 0; i--) {
            b[i] = (byte) ((d % 10) + '0');
            d /= 10;
        }
        return b;
    }

    public static byte[] format(final int number) {
        return format(number, 10);
    }

    private static double getMegaBytesPerSec(long durationInMs, long datasize) {
        double megabytesPerSec = (double) datasize * 1000 / durationInMs / 1024 / 1024;
        return megabytesPerSec;
    }

    static class BarTableTest extends Test {

        static final byte[] COLUMN_SYMBOL = Bytes.toBytes("a");
        static final byte[] COLUMN_TIME = Bytes.toBytes("b");
        static final byte[] COLUMN_BAR_LEN = Bytes.toBytes("c");
        static final byte[] COLUMN_FLAGS = Bytes.toBytes("d");
        static final byte[] COLUMN_OPEN = Bytes.toBytes("e");
        static final byte[] COLUMN_HIGH = Bytes.toBytes("f");
        static final byte[] COLUMN_LOW = Bytes.toBytes("g");
        static final byte[] COLUMN_LAST = Bytes.toBytes("h");
        static final byte[] COLUMN_VOLUME = Bytes.toBytes("i");

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
            Put put = new Put(format(i));

            // generate a string as a symbols (here the symbol is unique. In
            // real-life data, symbol is not unique)
            put.add(FAMILY_NAME, COLUMN_SYMBOL, format(i, 16));

            put.add(FAMILY_NAME, COLUMN_TIME, format(i));
            put.add(FAMILY_NAME, COLUMN_BAR_LEN, Bytes.toBytes(1));
            put.add(FAMILY_NAME, COLUMN_FLAGS, Bytes.toBytes(0));
            put.add(FAMILY_NAME, COLUMN_OPEN, Bytes.toBytes(1293.0));
            put.add(FAMILY_NAME, COLUMN_HIGH, Bytes.toBytes(1293.0));
            put.add(FAMILY_NAME, COLUMN_LOW, Bytes.toBytes(1293.0));
            put.add(FAMILY_NAME, COLUMN_LAST, Bytes.toBytes(1293.0));
            put.add(FAMILY_NAME, COLUMN_VOLUME, Bytes.toBytes(1));

            put.setWriteToWAL(writeToWAL);

            collectRowStatistics(48 + 16, put.heapSize());

            table.put(put);

        }

    }

    /*
     * Test Case 1.2 Scan 1200 records
     */
    static class BarFilteredScanTest extends BarTableTest {

        public final static int SCAN_START = 123;
        public final static int SCAN_ROWS = 1200;

        BarFilteredScanTest(Configuration conf) {
            super(
                  conf,
                  "[TestCase1.2][sBar SequentialFilteredScan Test(tTime is within a range, scan 1200 records)]",
                  new TestOptions(0, SBAR_COUNT, SBAR_TABLE_NAME));
        }

        @Override
        void testTimed() throws IOException {
            byte[] valueFrom = format(SCAN_START); // Bytes.toBytes(SCAN_START);
            byte[] valueTo = format(SCAN_START + SCAN_ROWS); // Bytes.toBytes(SCAN_START
                                                             // + SCAN_ROWS);
            Scan scan = constructScan(valueFrom, valueTo);
            ResultScanner scanner = null;
            try {
                scanner = this.table.getScanner(scan);

                Result result = null;
                while ((result = scanner.next()) != null) {
                    collectRowStatistics(48 + 16, result.getWritableSize());
                }
            } finally {
                if (scanner != null)
                    scanner.close();
            }
        }

        protected Scan constructScan(byte[] valueFrom, byte[] valueTo) throws IOException {
            Filter singleColumnValueFilterA = new SingleColumnValueFilter(FAMILY_NAME, COLUMN_TIME,
                CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(valueFrom));

            Filter singleColumnValueFilterB = new SingleColumnValueFilter(FAMILY_NAME, COLUMN_TIME,
                CompareFilter.CompareOp.LESS, new BinaryComparator(valueTo));

            FilterList filterlist = new FilterList(Operator.MUST_PASS_ALL,
                Arrays.asList((Filter) singleColumnValueFilterA, singleColumnValueFilterB));

            Scan scan = new Scan();
            // scan.addColumn(FAMILY_NAME, Bytes.toBytes("tTime"));
            scan.setFilter(filterlist);
            return scan;
        }

    }

    /*
     * Test Case 1.3 Random scan 100 records
     */
    static class BarRandomScanTest extends BarTableTest {

        BarRandomScanTest(Configuration conf) {
            super(conf, "[TestCase1.3][sBar Random Scan 100 Records(by symbols)]", new TestOptions(
                0, SBAR_COUNT, SBAR_TABLE_NAME));
        }

        @Override
        void testTimed() throws IOException {
            for (int i = 0; i < 100; i++) {
                scanBySymbol();
            }
        }

        private void scanBySymbol() throws IOException {
            byte[] valueFrom = format(rand.nextInt(SBAR_COUNT), 16);
            Scan scan = constructScan(valueFrom);
            ResultScanner scanner = null;
            try {
                scanner = this.table.getScanner(scan);

                Result result = null;
                while ((result = scanner.next()) != null) {
                    collectRowStatistics(48 + 16, result.getWritableSize());
                }
            } finally {
                if (scanner != null)
                    scanner.close();
            }
        }

        private Scan constructScan(byte[] value) throws IOException {
            Filter filter = new SingleColumnValueFilter(FAMILY_NAME, COLUMN_SYMBOL,
                CompareFilter.CompareOp.EQUAL, new BinaryComparator(value));

            Scan scan = new Scan();
            // scan.addColumn(FAMILY_NAME, Bytes.toBytes("tTime"));
            scan.setFilter(filter);
            return scan;
        }

    }

    static class TicTableTest extends Test {

        static final byte[] COLUMN_SYMBOL = Bytes.toBytes("a");
        static final byte[] COLUMN_TIME = Bytes.toBytes("b");
        static final byte[] COLUMN_FLAGS = Bytes.toBytes("c");

        // for quote
        static final byte[] COLUMN_BID_PRICE = Bytes.toBytes("d");
        static final byte[] COLUMN_ASK_PRICE = Bytes.toBytes("e");
        static final byte[] COLUMN_BID_SIZE = Bytes.toBytes("f");
        static final byte[] COLUMN_ASK_SIZE = Bytes.toBytes("g");
        static final byte[] COLUMN_BID_EXCHANGE = Bytes.toBytes("h");
        static final byte[] COLUMN_ASK_EXCHANGE = Bytes.toBytes("i");

        // for trade
        static final byte[] COLUMN_TRADE_PRICE = Bytes.toBytes("j");
        static final byte[] COLUMN_TRADE_VOLUME = Bytes.toBytes("k");
        static final byte[] COLUMN_CUM_VOLUME = Bytes.toBytes("l");
        static final byte[] COLUMN_TRADE_EXCHANGE = Bytes.toBytes("m");
        static final byte[] COLUMN_RESERVED = Bytes.toBytes("n");

        TicTableTest(final Configuration conf, final String testCaseName, final TestOptions options) {
            super(conf, testCaseName, options);
        }
    }

    /*
     * Test Case 2
     */
    static class TicQuoteSequentialWriteTest extends TicTableTest {

        static final long timestamp = 1312840920l;

        static final char ROW_KEY_DELIMITER = '_';
        static final char ROW_VALUE_DELIMITER = ',';

        TicQuoteSequentialWriteTest(Configuration conf) {
            super(conf, "[TestCase2][TickQuote SequentialWrite Test]", new TestOptions(0,
                recordCounter, STIK_RECORD_TABLE_NAME));

        }

        byte[] getRowKey(final int i) {
            StringBuilder sb = new StringBuilder();
            sb.append(SymbolLoader.randomSymbol());
            sb.append(ROW_KEY_DELIMITER);
            sb.append(timestamp + i);
            return sb.toString().getBytes();
        }

        @Override
        void testRow(final int i) throws IOException {
            // Put put = new Put(format(i));

            // generate a string as a symbols (here the symbol is unique. In
            // real-life data, symbol is not unique)
            // put.add(FAMILY_NAME, COLUMN_SYMBOL, format(i, 16));

            byte[] key = getRowKey(i);
            Put put = new Put(key);

            put.add(FAMILY_NAME, COLUMN_TIME, Bytes.toBytes(123));
            put.add(FAMILY_NAME, COLUMN_FLAGS, Bytes.toBytes(16));

            if (i % 2 == 0) {
                // assume this is quote
                // TIC_Quote
                // typedef struct TIC_QUOTE // 32 bytes
                // {
                // double dBidPrice;
                // double dAskPrice;
                // unsigned iBidSize;
                // unsigned iAskSize;
                // char cBidExchange[4];
                // char cAskExchange[4];
                // } sTicQuote;
                put.add(FAMILY_NAME, COLUMN_BID_PRICE, Bytes.toBytes(13.45d));
                put.add(FAMILY_NAME, COLUMN_ASK_PRICE, Bytes.toBytes(0.0d));
                put.add(FAMILY_NAME, COLUMN_BID_SIZE, Bytes.toBytes(1));
                put.add(FAMILY_NAME, COLUMN_ASK_SIZE, Bytes.toBytes(0));
                put.add(FAMILY_NAME, COLUMN_BID_EXCHANGE, Bytes.toBytes("NMS"));
                put.add(FAMILY_NAME, COLUMN_ASK_EXCHANGE, Bytes.toBytes("NMS"));
            } else {
                // assume this is trade
                // TIC_Trade
                // typedef struct TIC_TRADE // 24 bytes
                // {
                // double dTradePrice;
                // unsigned iTradeVolume;
                // unsigned uCumVolume;
                // char cTradeExchange[4];
                // char cReserved[4];
                // } sTicTrade;
                put.add(FAMILY_NAME2, COLUMN_TRADE_PRICE, Bytes.toBytes(13.45d));
                put.add(FAMILY_NAME2, COLUMN_TRADE_VOLUME, Bytes.toBytes(100));
                put.add(FAMILY_NAME2, COLUMN_CUM_VOLUME, Bytes.toBytes(1000));
                put.add(FAMILY_NAME2, COLUMN_TRADE_EXCHANGE, Bytes.toBytes("NMS"));
                put.add(FAMILY_NAME2, COLUMN_RESERVED, Bytes.toBytes("ABC"));
            }

            put.setWriteToWAL(writeToWAL);

            collectRowStatistics(40 + 16, put.heapSize());

            table.put(put);
        }

        @Override
        protected int getReportingPeriod() {
            return 50000;
        }
    }

    /*
     * Test Case 3:
     */
    static class TicQuoteSequentialReadTest extends TicTableTest {

        TicQuoteSequentialReadTest(Configuration conf) {
            super(conf, "[TestCase3][TickQuote SequentialRead Test]", new TestOptions(0,
                recordCounter, STIK_RECORD_TABLE_NAME));
        }

        @Override
        void testTimed() throws IOException {
            Scan scan = new Scan();
            ResultScanner scanner = null;
            try {
                scanner = this.table.getScanner(scan);
                Result result = null;
                while ((result = scanner.next()) != null) {
                    collectRowStatistics(40 + 16, result.getWritableSize());
                }
            } finally {
                if (scanner != null)
                    scanner.close();
            }
        }

        @Override
        protected int getReportingPeriod() {
            return 50000;
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


