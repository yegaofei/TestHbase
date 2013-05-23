package com.insigma.tickserver;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: May 23, 2013
 */

public class TestTickDataType {

    @Test
    public void testConversion() {

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

        byte[] tdtInArray = tdt.toBytes();

        TickDataType tdt2 = new TickDataType();
        tdt2.fromBytes(tdtInArray);

        /* Compare */
        Assert.assertArrayEquals(tdt.SecQualifiers, tdt2.SecQualifiers);
        Assert.assertEquals(tdt.Beacon, tdt2.Beacon);
        Assert.assertTrue(tdt.Ask.equals(tdt2.Ask));
        Assert.assertTrue(tdt.Bid.equals(tdt2.Bid));
        Assert.assertTrue(tdt.Trade.equals(tdt2.Trade));
        Assert.assertTrue(tdt.AuthCode == tdt2.AuthCode);
        Assert.assertTrue(tdt.Beacon == tdt2.Beacon);
        Assert.assertTrue(tdt.Category == tdt2.Category);
        Assert.assertTrue(tdt.ExchangeTime == tdt2.ExchangeTime);
        Assert.assertTrue(tdt.Flags == tdt2.Flags);
        Assert.assertTrue(tdt.LineID == tdt2.LineID);
        Assert.assertTrue(tdt.SequenceNumber == tdt2.SequenceNumber);
        Assert.assertTrue(tdt.SequenceSeries == tdt2.SequenceSeries);
        Assert.assertTrue(tdt.SubCategory == tdt2.SubCategory);
        Assert.assertTrue(tdt.VWap == tdt2.VWap);

        Assert.assertArrayEquals(tdt.toBytes(), tdt2.toBytes());

    }

}


