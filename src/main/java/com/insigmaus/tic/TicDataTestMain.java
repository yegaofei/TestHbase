package com.insigmaus.tic;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 27, 2013
 */

public class TicDataTestMain {

    private Configuration conf;

    private static final int SYMBOL_COUNT = 190009; // Totally we have 190009
    // symbols in
    // TicSymbolAndCount.txt, for
    // time-being reason, we don't
    // use all of them

    private static final int PROCESS_COUNT = 1;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private static final String TIC_TRADE_TABLE_NAME = "Tic_Trade";

    private static final String TIC_QUOTE_TABLE_NAME = "Tic_Quote";

    private static final String SYMBOL_TABLE_NAME = "Symbol";

    private static final byte[] SYMBOL_FAMILY_NAME = Bytes.toBytes("ID");

    private CountDownLatch countDownLatch;

    private SymbolCount[] symbolCountArray = null;

    public TicDataTestMain(Configuration conf) {
        this.conf = conf;
    }
    
    public static void main(String[] args) {
        TicDataTestMain testMain = new TicDataTestMain(HBaseConfiguration.create());

        try {
            testMain.createTables();


            TicDataGenerate tdg = new TicDataGenerate();
            try {
                tdg.readCountFile("TicSymbolAndCount.txt", SYMBOL_COUNT);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            }
            SymbolCount[] symbolCountArray = tdg.getSymbolCountArray();
            SymbolDataTool sydw = new SymbolDataTool(testMain.conf);
            sydw.processSymbolData(symbolCountArray);
            System.out.println("Symbol table has been populdated.");

            testMain.setSymbolCountArray(symbolCountArray);

            long startTime = System.currentTimeMillis();
            testMain.processTicData();
            System.out.println("Inserting " + SYMBOL_COUNT + " symbols by spending "
                    + (System.currentTimeMillis() - startTime));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void processTicData() {
        int lengthOfSegment = SYMBOL_COUNT / PROCESS_COUNT;
        while ((lengthOfSegment * PROCESS_COUNT) < SYMBOL_COUNT)
            lengthOfSegment++;

        countDownLatch = new CountDownLatch(PROCESS_COUNT);

        for (int i = 0; i < PROCESS_COUNT; i++) {
            int startIndex = i * lengthOfSegment;
            int endIndex = startIndex + lengthOfSegment;
            if (endIndex > SYMBOL_COUNT) {
                endIndex = SYMBOL_COUNT;
            }

            TicDataWriter tdw = new TicDataWriter(startIndex, endIndex);
            tdw.setCountDownLatch(countDownLatch);
            tdw.setSymbolCountArray(symbolCountArray);
            tdw.setConf(this.conf);

            Thread t = new Thread(tdw);
            t.start();
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void createTables() throws IOException {
        byte[][] sBarFamilyNames = { SYMBOL_FAMILY_NAME };
        createTable(SYMBOL_TABLE_NAME, sBarFamilyNames, 1, 1, 200000, 3);
        
        byte[][] sTicFamilyNames = { FAMILY_NAME };
        createTable(TIC_TRADE_TABLE_NAME, sTicFamilyNames, 3, 0, 800000000, 1000);

        byte[][] sQuoteFamilyNames = { FAMILY_NAME };
        createTable(TIC_QUOTE_TABLE_NAME, sQuoteFamilyNames, 3, 0, 800000000, 1000);
    }

    private void createTable(String tableName, byte[][] familyNames, int maxVersions, int startKey,
                             int endKey, int regionNum)
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


        byte[] sKey = Bytes.toBytes(startKey); // your lowest keuy
        byte[] eKey = Bytes.toBytes(endKey); // your highest key
        int numberOfRegions = regionNum; // # of regions to create
        admin.createTable(tableDescriptor, sKey, eKey, numberOfRegions);

        boolean tableAvailable = admin.isTableAvailable(tableName);
        if (tableAvailable) {
            System.out.println("table created:" + tableName);
        }
        admin.close();
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
        admin.close();
    }

    public SymbolCount[] getSymbolCountArray() {
        return symbolCountArray;
    }

    public void setSymbolCountArray(SymbolCount[] symbolCountArray) {
        this.symbolCountArray = symbolCountArray;
    }

}


