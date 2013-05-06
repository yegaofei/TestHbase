package com.insigmaus;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 27, 2013
 */

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import au.com.bytecode.opencsv.CSVReader;

/**
 * This class to test the HBase performance 3 test cases: 1. Bar data read 2.
 * Tic data read 3. Tic data write
 **/

public class MZTest {

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private static final String BAR = "B";

    private static final String SYMBOL = "S";

    private static final String TIC = "T";

    private Configuration conf;
    private HBaseAdmin admin;
    List<String> TableNames;

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        MZTest test = new MZTest();
        test.init();
        try {
            test.loadBarData();
            System.exit(-2);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(-2);
        }
    }

    // ///////////////////////////////////////////////////////
    // 0. All init resource work
    // //////////////////////////////////////////////////////
    public void init() {
        conf = HBaseConfiguration.create();
        TableNames = new LinkedList<String>();
        try {
            admin = new HBaseAdmin(this.conf);
        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(-1);
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(-1);
        }
    }

    // ///////////////////////////////////////////////////////
    // 1. Prepare : Generate Bar data from CSV file
    // //////////////////////////////////////////////////////
    public void loadBarData() throws IOException {

        CSVReader reader = new CSVReader(new FileReader("SampleBarData.csv"));
        String[] n;
        // Skip Header
        reader.readNext();
        List<String[]> bars = new LinkedList<String[]>();
        // Keep all data in memory
        while ((n = reader.readNext()) != null) {
            bars.add(n);
        }
        reader.close();

        // write to tables
        HTable sTable = new HTable(conf, SYMBOL);
        HTable bTable = new HTable(conf, BAR);

        // 1. Construct puts for both symbol and bar
        Iterator<String[]> it = bars.iterator();
        int i = 1000000000;
        String symbol = "";

        List<Put> symbolPuts = new LinkedList<Put>();
        List<Put> barPuts = new LinkedList<Put>();

        // Construct symbol and Bar
        while (it.hasNext()) {
            String[] s = it.next();

            Put _pS = new Put(Bytes.toBytes(java.lang.Math.abs(s[0].hashCode()) * i
                    + (Integer.valueOf(s[1]) / 60)));
            _pS.add(Bytes.toBytes("c"), Bytes.toBytes("f"), Long.valueOf(s[1]), Bytes.toBytes(s[0])); // ID
                                                                                                      // ->
                                                                                                      // Symbol

            Put _pB = new Put(Bytes.toBytes(s[0].hashCode() * i + (Integer.valueOf(s[1]) / 60))); // ID
                                                                                                  // (rowkey)
            _pB.add(Bytes.toBytes("c"), Bytes.toBytes("f"), Long.valueOf(s[1]), Bytes.toBytes(0)); // Flag
            _pB.add(Bytes.toBytes("c"), Bytes.toBytes("f"), Long.valueOf(s[1]), Bytes.toBytes(s[2])); // Flag
            _pB.add(Bytes.toBytes("c"), Bytes.toBytes("h"), Long.valueOf(s[1]), Bytes.toBytes(s[3])); // High
            _pB.add(Bytes.toBytes("c"), Bytes.toBytes("a"), Long.valueOf(s[1]), Bytes.toBytes(s[4])); // Last
            _pB.add(Bytes.toBytes("c"), Bytes.toBytes("l"), Long.valueOf(s[1]), Bytes.toBytes(s[5])); // Low
            _pB.add(Bytes.toBytes("c"), Bytes.toBytes("o"), Long.valueOf(s[1]), Bytes.toBytes(s[6])); // Open
            _pB.add(Bytes.toBytes("c"), Bytes.toBytes("b"), Long.valueOf(s[1]), Bytes.toBytes(s[7])); // Bar
                                                                                                      // length
            _pB.add(Bytes.toBytes("c"), Bytes.toBytes("v"), Long.valueOf(s[1]), Bytes.toBytes(s[8])); // Volume

            symbolPuts.add(_pS);
            barPuts.add(_pB);
        }

        // Do actual write
        sTable.setAutoFlush(false);
        bTable.setAutoFlush(false);
        sTable.put(symbolPuts);

        long startTime = System.currentTimeMillis();

        bTable.put(barPuts);

        long endTime = System.currentTimeMillis();

        System.out.println("Total Record write into is: " + symbolPuts.size() + " total time is: "
                + (endTime - startTime) + " ms.");

    }

}

