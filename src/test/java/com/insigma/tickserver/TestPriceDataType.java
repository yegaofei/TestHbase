package com.insigma.tickserver;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: May 23, 2013
 */

public class TestPriceDataType extends TestCase {

    @Test
    public void testConversion() {

        PriceDataType pdt = new PriceDataType();
        pdt.exchange = Bytes.toBytes("NYSE");
        pdt.flags = 2L;
        pdt.qualifiers = Bytes.toBytes("abcd");
        pdt.size = 500;
        pdt.ucumvolume = 600;
        pdt.value = 20.3;
        pdt.volqualifiers = Bytes.toBytes("ef");

        byte[] bytes = pdt.toBytes();

        PriceDataType pdt2 = new PriceDataType();
        pdt2.fromBytes(bytes);

        /* compare */
        Assert.assertArrayEquals(pdt.exchange, pdt2.exchange);
        Assert.assertEquals(pdt.flags, pdt2.flags);
        Assert.assertArrayEquals(pdt.qualifiers, pdt2.qualifiers);
        Assert.assertEquals(pdt.size, pdt2.size);
        Assert.assertEquals(pdt.ucumvolume, pdt2.ucumvolume);
        Assert.assertTrue(pdt.value == pdt2.value);
        Assert.assertArrayEquals(pdt.volqualifiers, pdt2.volqualifiers);
        Assert.assertTrue(pdt.equals(pdt2));

        Assert.assertArrayEquals(pdt.toBytes(), pdt2.toBytes());

    }

}


