package com.insigma.tickserver;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.hbase.util.Bytes;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Jun 3, 2013
 */

public class EndienConvertion {

    public static byte[] reverseBytes(byte[] inArray) {
        if (inArray == null) {
            return null;
        }

        byte[] outArray = new byte[inArray.length];

        for (int i = 0; i < inArray.length; i++) {
            outArray[inArray.length - 1 - i] = inArray[i];
        }

        return outArray;
    }

    /**
     * Convert the unsigned integer value in given byte order to signed long
     * value in big endian byte order
     * 
     * @param unsignedIntValue
     * @param bo
     *            , ByteOrder.LITTLE_ENDIAN or ByteOrder.BIG_ENDIAN
     * @return
     */
    public static Long unsignedIntToLong(byte[] unsignedIntValue, ByteOrder bo) {
        if (unsignedIntValue == null) {
            return null;
        }

        if (unsignedIntValue.length != 4) {
            throw new RuntimeException("Unsigned int value is supposed to be 4 bytes long");
        }

        ByteBuffer bb_unsignedIntVar = ByteBuffer.wrap(unsignedIntValue).order(bo);
        byte[] intArray = Bytes.toBytes(bb_unsignedIntVar.getInt());
        byte[] longArray = new byte[8];
        System.arraycopy(intArray, 0, longArray, 4, intArray.length);
        return Bytes.toLong(longArray);
    }

    /**
     * Convert the unsigned short value in given byte order to signed int value
     * in big endian byte order
     * 
     * @param unsignedShortValue
     * @param bo
     *            ByteOrder.LITTLE_ENDIAN or ByteOrder.BIG_ENDIAN
     * @return
     */
    public static Integer unsignedShortToInt(byte[] unsignedShortValue, ByteOrder bo) {
        if (unsignedShortValue == null) {
            return null;
        }

        if (unsignedShortValue.length != 2) {
            throw new RuntimeException("Unsigned int value is supposed to be 2 bytes long");
        }

        ByteBuffer bb_unsignedShortVar = ByteBuffer.wrap(unsignedShortValue).order(bo);
        byte[] shortArray = Bytes.toBytes(bb_unsignedShortVar.getShort());
        byte[] intArray = new byte[4];
        System.arraycopy(shortArray, 0, intArray, 2, shortArray.length);
        return Bytes.toInt(intArray);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Long l1 = 1L;
        byte[] l1inbyte = Bytes.toBytes(l1);
        for (int i = 0; i < l1inbyte.length; i++) {
            System.out.print(l1inbyte[i]);
        }
        System.out.println("");

        byte[] l2inbyte = reverseBytes(l1inbyte);

        for (int i = 0; i < l2inbyte.length; i++) {
            System.out.print(l2inbyte[i]);
        }
        System.out.println("");

    }

}


