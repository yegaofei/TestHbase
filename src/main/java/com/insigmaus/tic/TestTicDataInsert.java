package com.insigmaus.tic;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

class TestTicDataInsert {

    private static final int SBAR_COUNT = 0;

    private static final int SYMBOL_COUNT = 190; // Totally we have 190009
                                                  // symbols in
                                                  // TicSymbolAndCount.txt, for
                                                  // time-being reason, we don't
                                                  // use all of them

    private static final int HTABLE_BUFFER_SIZE = 1024 * 1024 * 20;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private static final String TIC_TABLE_NAME = "Tic_Trade";

    private static final String SYMBOL_TABLE_NAME = "Symbol";

    private static final byte[] SYMBOL_FAMILY_NAME = Bytes.toBytes("ID");

    private static final String TIC_QUOTE_TABLE_NAME = "Tic_Quote";

    private static final byte[] SYMBOL_ID_QUALIFIER_NAME = Bytes.toBytes("S");

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
        System.out.println("\t1 = Tic data Write test (" + SYMBOL_COUNT + " symbols)");
        System.out.println("\t2 = Tic data Read test");
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

                new TestTicDataInsert(HBaseConfiguration.create()).runTests(cmds);

            } catch (Exception e) {
                if (yourSelected == 2) {
                    System.out.println("There is no data in table bar, please select 1 to insert data at first.");
                } else {
                    throw e;
                }
            }

        }

    }

    public TestTicDataInsert(Configuration conf) {
        this.conf = conf;
    }

    private void runTests(final List<Class<? extends Test>> cmds) throws Exception {
        // setupTables();

        for (Class<? extends Test> cmd : cmds) {
            if (yourSelected == 1) {
                if (!tableExists(TIC_TABLE_NAME)) {
                    createTable(TIC_TABLE_NAME);
                }

                if (!tableExists(SYMBOL_TABLE_NAME)) {
                    createTable(SYMBOL_TABLE_NAME);
                }

                if (!tableExists(TIC_QUOTE_TABLE_NAME)) {
                    createTable(TIC_QUOTE_TABLE_NAME);
                }
            } else if (yourSelected == 2) {
                if (!tableExists(TIC_TABLE_NAME)) {
                    throw new Exception("no table");
                }
            }
            runTestForMultipleRounds(cmd);
        }

        // teardownTables();
    }

    public void createTable(String table) throws IOException {
        if (table.equals(SYMBOL_TABLE_NAME)) {
            byte[][] sBarFamilyNames = { SYMBOL_FAMILY_NAME };
            createTable(table, sBarFamilyNames, 1);
        } else {
            byte[][] sBarFamilyNames = { FAMILY_NAME };
            createTable(table, sBarFamilyNames, 10000);
        }
    }

    public void dropTable(String table) throws IOException {
        deleteTable(table);
    }

    void runTestForMultipleRounds(final Class<? extends Test> cmd) throws IOException {
        System.out.println("****Test Case " + yourSelected + " to run: ");
        double totalStatistic = 0;
        totalStatistic += runTest(cmd);

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

    private void createTable(String tableName, byte[][] familyNames, int maxVersions)
                                                                                     throws IOException {
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
            HColumnDescriptor hd = new HColumnDescriptor(familyName);
            hd.setMaxVersions(maxVersions);
            hd.setInMemory(true);
            tableDescriptor.addFamily(hd);
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

    static class BarTableTest extends Test {

        static final byte[] COLUMN_TIME = Bytes.toBytes("TimeStamp");
        static final byte[] COLUMN_FLAGS = Bytes.toBytes("flag");
        static final byte[] COLUMN_EXH_TIME = Bytes.toBytes("ExchangeTime");
        static final byte[] COLUMN_SEQUENCENUMBER = Bytes.toBytes("SequenceNumber");
        static final byte[] COLUMN_LINEID = Bytes.toBytes("LineID");
        static final byte[] COLUMN_SEQUENCESEIRES = Bytes.toBytes("SequenceSeires");
        static final byte[] COLUMN_SEQUENCEQAULIFIER = Bytes.toBytes("SequenceQaulifier");
        static final byte[] COLUMN_TRADEEXCHANGE = Bytes.toBytes("TradeExchange");
        static final byte[] COLUMN_TRADEPRICE = Bytes.toBytes("TradePrice");
        static final byte[] COLUMN_VWAP = Bytes.toBytes("VWAP");
        static final byte[] COLUMN_TRADEVOLUME = Bytes.toBytes("TradeVolume");
        static final byte[] COLUMN_QUALIFIERS = Bytes.toBytes("Qualifiers");
        static final byte[] COLUMN_CUMVOLUME = Bytes.toBytes("CumVolume");
        static final byte[] COLUMN_VOLQUALIFIERS = Bytes.toBytes("Volqualifiers");
        static final byte[] COLUMN_ASK_EXCHANGE = Bytes.toBytes("AskExchange");
        static final byte[] COLUMN_BID_EXCHANGE = Bytes.toBytes("BidExchange");
        static final byte[] COLUMN_BID_PRICE = Bytes.toBytes("BidPrice");
        static final byte[] COLUMN_ASK_PRICE = Bytes.toBytes("AskPrice");
        static final byte[] COLUMN_BID_SIZE = Bytes.toBytes("BidSize");
        static final byte[] COLUMN_ASK_SIZE = Bytes.toBytes("AskSize");

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
                TIC_TABLE_NAME));

        }

        @Override
        void testRow(final int i) throws IOException {
            // System.out.println("Starting testRow() " + i + " at " +
            // System.currentTimeMillis());
            SymbolCount symbolCount = super.symbolCountArray[i];
            String symbol = symbolCount.getSymbol();
            int count = symbolCount.getCount();

            //insert into symbol table
            insertSymbolData(symbol, i);
            // System.out.println("insertSymbolData completes  at " +
            // System.currentTimeMillis());

            int tradeCount = count / 10;
            TicTradeCID[] ticTrade = super.tdg.generateTicTradeCID(symbol, tradeCount);

            List<Put> putList = new LinkedList<Put>();
            for (int k = 0; k < ticTrade.length; k++) {
                TicTradeCID trade = ticTrade[k];
                byte[] key = Bytes.toBytes(RowKeyGenerator.generateRowKey(i, trade.gettTime()));
                Put put = new Put(key);
                put.add(FAMILY_NAME, COLUMN_TIME, trade.gettTime(), Bytes.toBytes(trade.gettTime()));
                put.add(FAMILY_NAME, COLUMN_FLAGS, trade.gettTime(),
                        Bytes.toBytes(trade.getuFlags()));
                put.add(FAMILY_NAME, COLUMN_EXH_TIME, trade.gettTime(),
                        Bytes.toBytes(trade.getExchangeTime()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCENUMBER, trade.gettTime(),
                        Bytes.toBytes(trade.getSequenceNumber()));
                put.add(FAMILY_NAME, COLUMN_LINEID, trade.gettTime(),
                        Bytes.toBytes(trade.getLineID()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCESEIRES, trade.gettTime(),
                        Bytes.toBytes(trade.getSequenceSeries()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCEQAULIFIER, trade.gettTime(),
                        trade.getSecqualifiers());
                put.add(FAMILY_NAME, COLUMN_TRADEEXCHANGE, trade.gettTime(),
                        new String(trade.getcTradeExchange()).getBytes());
                put.add(FAMILY_NAME, COLUMN_TRADEPRICE, trade.gettTime(),
                        Bytes.toBytes(trade.getdTradePrice()));
                put.add(FAMILY_NAME, COLUMN_VWAP, trade.gettTime(), Bytes.toBytes(trade.getdVWAP()));
                put.add(FAMILY_NAME, COLUMN_TRADEVOLUME, trade.gettTime(),
                        Bytes.toBytes(trade.getiTradeVolume()));
                put.add(FAMILY_NAME, COLUMN_QUALIFIERS, trade.gettTime(), trade.getQualifiers());
                put.add(FAMILY_NAME, COLUMN_CUMVOLUME, trade.gettTime(),
                        Bytes.toBytes(trade.getuCumVolume()));
                put.add(FAMILY_NAME, COLUMN_VOLQUALIFIERS, trade.gettTime(),
                        trade.getVolqualifiers());

                // put.setWriteToWAL(writeToWAL);
                putList.add(put);
            }
            table.put(putList);
            // System.out.println("insert TicTradeCID completes  at " +
            // System.currentTimeMillis());

            long tStart = System.currentTimeMillis();
            TicQuoteCID[] ticQuote = super.tdg.generateTicQuoteCIDArray(symbol, count - tradeCount);
            List<Put> putListQuote = new LinkedList<Put>();
            for (int k = 0; k < ticQuote.length; k++) {
                TicQuoteCID quote = ticQuote[k];
                byte[] key = Bytes.toBytes(RowKeyGenerator.generateRowKey(i, quote.gettTime()));
                Put put = new Put(key);
                put.add(FAMILY_NAME, COLUMN_TIME, quote.gettTime(), Bytes.toBytes(quote.gettTime()));
                put.add(FAMILY_NAME, COLUMN_FLAGS, quote.gettTime(),
                        Bytes.toBytes(quote.getuFlags()));
                put.add(FAMILY_NAME, COLUMN_EXH_TIME, quote.gettTime(),
                        Bytes.toBytes(quote.getExchangeTime()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCENUMBER, quote.gettTime(),
                        Bytes.toBytes(quote.getSequenceNumber()));
                put.add(FAMILY_NAME, COLUMN_LINEID, quote.gettTime(),
                        Bytes.toBytes(quote.getLineID()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCESEIRES, quote.gettTime(),
                        Bytes.toBytes(quote.getSequenceSeries()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCEQAULIFIER, quote.gettTime(),
                        quote.getSecqualifiers());
                put.add(FAMILY_NAME, COLUMN_ASK_EXCHANGE, quote.gettTime(),
                        new String(quote.getcAskExchange()).getBytes());
                put.add(FAMILY_NAME, COLUMN_BID_EXCHANGE, quote.gettTime(),
                        new String(quote.getcBidExchange()).getBytes());
                put.add(FAMILY_NAME, COLUMN_BID_PRICE, quote.gettTime(), Bytes.toBytes(quote.getdBidPrice()));
                put.add(FAMILY_NAME, COLUMN_ASK_PRICE, quote.gettTime(),
                        Bytes.toBytes(quote.getdAskPrice()));
                put.add(FAMILY_NAME, COLUMN_BID_SIZE, quote.gettTime(), Bytes.toBytes(quote.getiBidSize()));
                put.add(FAMILY_NAME, COLUMN_ASK_SIZE, quote.gettTime(),
                        Bytes.toBytes(quote.getiAskSize()));
               
                // put.setWriteToWAL(writeToWAL);
                putListQuote.add(put);

                if (k % 500 == 0 && k > 1) {
                    super.ticQuoteTable.put(putListQuote);
                    putListQuote = new LinkedList<Put>();
                }
            }
            if (putListQuote.size() > 0) {
                super.ticQuoteTable.put(putListQuote);
            }

            System.out.println("insert TicQuoteCID " + putListQuote.size()
                    + " completes by spending "
                    + (System.currentTimeMillis() - tStart));
        }

        // we don't yet check if the symbol exists or not.
        private void insertSymbolData(String symbol, int id) throws IOException {
            // System.out.println("Start inserting symbol " + symbol +
            // " with id " + id);
            byte[] key = symbol.getBytes();
            Put put = new Put(key);
            put.add(SYMBOL_FAMILY_NAME, SYMBOL_ID_QUALIFIER_NAME, Bytes.toBytes(id));
            put.setWriteToWAL(writeToWAL);
            super.symbolTable.put(put);
            // System.out.println("End inserting symbol " + symbol + " with id "
            // + id);
        }
    }

    static class BarFilteredScanTest extends BarTableTest {

        public final static int SCAN_START = 123;
        public final static int SCAN_ROWS = 1200;

        BarFilteredScanTest(Configuration conf) {
            super(conf, "[TestCase 2][sBar Find By Symbol Test]", new TestOptions(0, SBAR_COUNT,
                TIC_TABLE_NAME));
        }

        private double calcPerformance(long timeSpend, int countFound) {
            double performance = ((double) countFound / (double) timeSpend) * 1000;
            return performance;
        }

        @Override
        void testTimed() throws IOException {
            int countFound = 0;
            int symbolCountForRead = (int) (SYMBOL_COUNT * 0.5); // Choose one
                                                                 // half of
                                                                  // SYMBOL_COUNT
                                                                  // for
                                                                  // fetch
                                                                  // testing
            if (super.symbolCountArray != null) {
                long startTime = System.currentTimeMillis();
                // we fetch symbols from this array randomly
                for (int i = 0; i < symbolCountForRead; i++) {
                    int index = (int) (Math.random() * symbolCountArray.length);
                    if (index == symbolCountArray.length)
                        index = symbolCountArray.length - 1;
                    SymbolCount sc = symbolCountArray[index];

                    String symbol = sc.getSymbol();
                    int count = sc.getCount();
                    int timeStamp = TicDataGenerate.getExchangeBaseTime()
                            + (int) (Math.random() * count);

                    Get getSymbolId = new Get(symbol.getBytes());
                    Result r = super.symbolTable.get(getSymbolId);
                    
                    KeyValue kv = r.getColumnLatest(SYMBOL_FAMILY_NAME, SYMBOL_ID_QUALIFIER_NAME);
                    if (kv != null) {
                        byte[] v = kv.getValue();
                        int symbolId = Bytes.toInt(v);
                        // System.out.println("Get Symbol " + symbol + " id is "
                        // + symbolId);
                        long key = RowKeyGenerator.generateRowKey(symbolId, timeStamp);
                        Get get = new Get(Bytes.toBytes(key));
                        get.setTimeRange(TicDataGenerate.getExchangeBaseTime(),
                                         TicDataGenerate.getExchangeBaseTime() + count);
                        Result quoteResult = super.ticQuoteTable.get(get);
                        // KeyValue seqNum =
                        // quoteResult.getColumnLatest(FAMILY_NAME,
                        // COLUMN_SEQUENCENUMBER);
                        // KeyValue exh =
                        // quoteResult.getColumnLatest(FAMILY_NAME,
                        // COLUMN_ASK_EXCHANGE);
                        // byte[] exhName = exh.getValue();
                        // System.out.println("Get Symbol KEY " + key +
                        // " exchange name is "
                        // + new String(exhName));
                        // if (exhName != null) {
                        // countFound++;
                        // }

                        Result tradeResult = super.table.get(get);
                        if (quoteResult.isEmpty() && tradeResult.isEmpty()) {
                            System.out.println("No result found ");
                            continue;
                        } else {
                            countFound += quoteResult.size();
                            System.out.println("quoteResult.size() : " + quoteResult.size());
                            countFound += tradeResult.size();
                            System.out.println("tradeResult.size() : " + tradeResult.size());
                        }

                    } else {
                        // symbol id is not found
                        continue;
                    }
                }
                long timeSpend = System.currentTimeMillis() - startTime;
                double perf = calcPerformance(timeSpend, countFound);
                System.out.println("Fetch " + countFound + " rows data, with spend " + timeSpend
                        + " millils sec");
                System.out.println("The performance is " + perf + " rows per sencond");
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
        protected HTable symbolTable;
        protected HTable ticQuoteTable;
        protected volatile Configuration conf;
        protected boolean flushCommits;
        protected boolean writeToWAL;



        /**
         * Size of values (size of the original C structure), without the
         * overhead
         */
        protected int rowCounter = 0;


        private long startTime = 0;

        protected TicDataGenerate tdg = new TicDataGenerate();

        protected SymbolCount[] symbolCountArray;

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
            this.symbolTable = null;
            this.ticQuoteTable = null;
            this.conf = conf;
            this.flushCommits = true;
            this.writeToWAL = true;
        }

        protected int getReportingPeriod() {
            return this.totalRows;
        }

        void testSetup() throws IOException {
            this.admin = new HBaseAdmin(conf);
            this.table = new HTable(conf, Bytes.toBytes(tableName));
            this.table.setWriteBufferSize(HTABLE_BUFFER_SIZE);
            this.table.setAutoFlush(false);
            // this.table.setScannerCaching(30);

            this.symbolTable = new HTable(conf, SYMBOL_TABLE_NAME);
            this.symbolTable.setAutoFlush(false);

            this.ticQuoteTable = new HTable(conf, TIC_QUOTE_TABLE_NAME);
            this.ticQuoteTable.setWriteBufferSize(HTABLE_BUFFER_SIZE);
            this.ticQuoteTable.setAutoFlush(false);

            // System.out.println("=================> Start reading TicSymbolAndCount.txt");
            tdg.readCountFile("TicSymbolAndCount.txt", SYMBOL_COUNT);
            symbolCountArray = tdg.getSymbolCountArray();
            // System.out.println("=================> End reading TicSymbolAndCount.txt "
            // + symbolCountArray.length);
        }

        void testTakedown() throws IOException {
            if (flushCommits) {
                this.table.flushCommits();
                this.ticQuoteTable.flushCommits();
                this.symbolTable.flushCommits();
            }
            this.table.close();
            this.ticQuoteTable.close();
            this.symbolTable.close();
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
            testSetup();

            try {
                testTimed();

            } finally {
                testTakedown();

                // statisticToReturn = outputStatistics(elapsedTime);
            }

            return statisticToReturn;
        }

        /**
         * Provides an extension point for tests that don't want a per row
         * invocation.
         */
        void testTimed() throws IOException {
            startTime = System.currentTimeMillis();
            for (int i = 0; i < symbolCountArray.length; i++) {
                testRow(i);
            }
            long elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println(symbolCountArray.length + " symbols Tic data have been inserted by "
                    + elapsedTime + " milli seconds");
        }

        /*
         * Test for individual row.
         * 
         * @param i Row index.
         */
        void testRow(final int i) throws IOException {}
    }

}


