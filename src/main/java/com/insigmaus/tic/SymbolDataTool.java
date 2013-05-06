package com.insigmaus.tic;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 28, 2013
 */

public class SymbolDataTool {

    private static final String SYMBOL_TABLE_NAME = "Symbol";

    private static final byte[] SYMBOL_FAMILY_NAME = Bytes.toBytes("ID");

    private static final byte[] SYMBOL_START_QUALIFIER_NAME = Bytes.toBytes("s");

    private static final byte[] SYMBOL_END_QUALIFIER_NAME = Bytes.toBytes("e");

    private HTable symbolTable;

    private Configuration conf;

    public SymbolDataTool(Configuration conf) {
        this.conf = conf;
        try {
            initTables();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void processSymbolData(SymbolCount[] symbolCountArray) throws IOException {
        int i = 0;
        List<Put> putList = new LinkedList<Put>();
        for (SymbolCount sc : symbolCountArray) {
            byte[] key = sc.getSymbol().getBytes();
            Put put = new Put(key);
            put.add(SYMBOL_FAMILY_NAME, SYMBOL_START_QUALIFIER_NAME, Bytes.toBytes(i));
            put.add(SYMBOL_FAMILY_NAME, SYMBOL_END_QUALIFIER_NAME, Bytes.toBytes(i + sc.getCount()));
            putList.add(put);
            i = i  + sc.getCount() + 1;
        }
        this.symbolTable.put(putList);
        this.symbolTable.flushCommits();
        this.symbolTable.close();
    }

    private void initTables() throws IOException {
        this.symbolTable = new HTable(conf, SYMBOL_TABLE_NAME);
        this.symbolTable.setAutoFlush(false);
    }

    public SymbolData searchSymbolData(String symbol) throws IOException {
        Get getSymbolId = new Get(symbol.getBytes());
        Result r = symbolTable.get(getSymbolId);
        byte[] startKey = r.getValue(SYMBOL_FAMILY_NAME, SYMBOL_START_QUALIFIER_NAME);
        byte[] endKey = r.getValue(SYMBOL_FAMILY_NAME, SYMBOL_END_QUALIFIER_NAME);

        if (startKey == null || endKey == null) {
            return null;
        }

        return new SymbolData(symbol, Bytes.toInt(startKey), Bytes.toInt(endKey));
    }


}


