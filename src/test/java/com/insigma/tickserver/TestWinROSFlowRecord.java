package com.insigma.tickserver;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: May 23, 2013
 */

public class TestWinROSFlowRecord {

    @Test
    public void testConversion() {
        WinROSFlowRecord wrf = new WinROSFlowRecord();
        wrf.dwPostSignature = 0x1234L;
        wrf.dwPreSignature = 0x5678L;
        wrf.RecordLength = 8;
        wrf.RecordType = 4;
        wrf.Sequence = 0x1111L;
        byte[] symBytes = new byte[48];
        symBytes[43] = '$';
        symBytes[44] = 'T';
        symBytes[45] = 'E';
        symBytes[46] = 'S';
        symBytes[47] = 'T';
        wrf.Symbol = new String(symBytes);

        wrf.Unused = (byte) 1;
        wrf.TickData = prepareTickDataType();

        byte[] bytes = wrf.toBytes();

        WinROSFlowRecord wrf2 = new WinROSFlowRecord();
        wrf2.fromBytes(bytes);

        Assert.assertTrue(wrf.dwPostSignature == wrf2.dwPostSignature);
        Assert.assertTrue(wrf.dwPreSignature == wrf2.dwPreSignature);
        Assert.assertTrue(wrf.RecordLength == wrf2.RecordLength);
        Assert.assertTrue(wrf.RecordType == wrf2.RecordType);
        Assert.assertTrue(wrf.Sequence == wrf2.Sequence);
        Assert.assertTrue(wrf.Symbol.equals(wrf2.Symbol));
        Assert.assertTrue(wrf.Unused == wrf2.Unused);
        Assert.assertArrayEquals(wrf.TickData.toBytes(), wrf2.TickData.toBytes());
                
    }

    private TickDataType prepareTickDataType() {

        TickDataType tdt = new TickDataType();
        tdt.AuthCode = 5;
        tdt.Beacon = 0xAAAAL;
        tdt.Category = (byte) 1;
        tdt.ExchangeTime = 0x1234L;
        tdt.Flags = 2;
        tdt.LineID = 3;
        tdt.SecQualifiers = Bytes.toBytes("abcd");
        tdt.SequenceNumber = 0x1111L;
        tdt.SequenceSeries = (byte) 4;
        tdt.SubCategory = (byte) 5;
        tdt.VWap = 55.5d;

        PriceDataType bid = new PriceDataType();
        bid.exchange = Bytes.toBytes("NYSE");
        bid.flags = 2L;
        bid.qualifiers = Bytes.toBytes("abcd");
        bid.size = 500;
        bid.ucumvolume = 600;
        bid.value = 20.1;
        bid.volqualifiers = Bytes.toBytes("ef");

        tdt.Bid = bid;

        PriceDataType ask = new PriceDataType();
        ask.exchange = Bytes.toBytes("NYSE");
        ask.flags = 2L;
        ask.qualifiers = Bytes.toBytes("abcd");
        ask.size = 500;
        ask.ucumvolume = 600;
        ask.value = 20.3;
        ask.volqualifiers = Bytes.toBytes("ef");

        tdt.Ask = ask;

        PriceDataType trade = new PriceDataType();
        trade.exchange = Bytes.toBytes("NYSE");
        trade.flags = 2L;
        trade.qualifiers = Bytes.toBytes("abcd");
        trade.size = 500;
        trade.ucumvolume = 600;
        trade.value = 20.2;
        trade.volqualifiers = Bytes.toBytes("ef");

        tdt.Trade = trade;

        return tdt;
    }
}


