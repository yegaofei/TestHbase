package com.insigma.tickserver;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 27, 2013
 */

public class WRFTestWriteMain {

    private Configuration conf;

    private static final int PROCESS_COUNT = 1;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    // public static final byte[] PRICEDATA_FAMILY_NAME = Bytes.toBytes("pf");

    private static final String WRF_TABLE_NAME = "WinROSFlowRecord";

    private CountDownLatch countDownLatch;

    private String flowrecords = null;

    public WRFTestWriteMain(Configuration conf) {
        this.conf = conf;
    }
    
    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Please input the flowrecords file path and name");
            System.exit(-1);
        }

    	Configuration conf = HBaseConfiguration.create();
        // conf.set("hbase.zookeeper.quorum", "10.0.37.20");
        WRFTestWriteMain testMain = new WRFTestWriteMain(conf);
        testMain.setFlowrecords(args[0]);

        try {
            testMain.createTables();

            // long startTime = System.currentTimeMillis();
            testMain.processWRFData();
            // System.out.println("Inserting  by spending "
            // + (System.currentTimeMillis() - startTime));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void processWRFData() {

        countDownLatch = new CountDownLatch(PROCESS_COUNT);

        for (int i = 0; i < PROCESS_COUNT; i++) {

            WRFDataWriter tdw = new WRFDataWriter(flowrecords);
            tdw.setCountDownLatch(countDownLatch);
            
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
        byte[][] sBarFamilyNames = { FAMILY_NAME };
        createTable(WRF_TABLE_NAME,
                    sBarFamilyNames,
                    3,
                    Bytes.toBytes(RegionInfo.MAX_ROW_PER_REGION),
                    Bytes.toBytes(RegionInfo.MAX_ROW_PER_REGION
                            * ((long) RegionInfo.REGION_COUNT - 1)), RegionInfo.REGION_COUNT);
    }
    
    private void createTable(String tableName, byte[][] familyNames, int maxVersions) throws IOException {

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
            hd.setCompressionType(Compression.Algorithm.LZO);
            tableDescriptor.addFamily(hd);
        }
        tableDescriptor.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
        admin.createTable(tableDescriptor);

        boolean tableAvailable = admin.isTableAvailable(tableName);
        if (tableAvailable) {
            System.out.println("table created:" + tableName);
        }
        admin.close();
    }

    private void createTable(String tableName, byte[][] familyNames, int maxVersions, byte[] startKey, byte[] endKey, int regionNum)
                                                                                     throws IOException {
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
       // tableDescriptor.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
        admin.createTable(tableDescriptor, startKey, endKey, regionNum);

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

    public String getFlowrecords() {
        return flowrecords;
    }

    public void setFlowrecords(String flowrecords) {
        this.flowrecords = flowrecords;
    }

}


