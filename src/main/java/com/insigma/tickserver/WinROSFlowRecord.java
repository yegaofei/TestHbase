package com.insigma.tickserver;

import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * FLOWRECORD_WINROS
 * 
 * @author gpan
 *
 */
public class WinROSFlowRecord {
	public static int RECORD_SIZE = 188;
	
	public long	dwPreSignature;	/* sync verification -- FR_PRESIG -- */
	public byte	RecordType;		/* Identifies following record type */
	public byte	Unused;			/* for ??? in future */
	public int	RecordLength;	/* Total length of data rec in case different recs in future */
	public long	Sequence;		/* Counter for verifying buffer reliability */
	public String Symbol;		/* Symbol for record */
	public TickDataType	TickData;
	public long	dwPostSignature;	/* sync verification -- FR_POSTSIG -- */	
	
	public WinROSFlowRecord() {
		dwPreSignature = Sequence = dwPostSignature = 0;
		RecordLength = 0;
		//Symbol = new char[48];
		TickData = new TickDataType();
	}
	
	/**
	 * Convert from binary record to structure
	 * @param raw
	 * @throws IllegalArgumentException
	 */
	public void fromBytes(byte []raw) throws IllegalArgumentException {
		if (raw == null || raw.length < RECORD_SIZE) {
			throw new java.lang.IllegalArgumentException("Insufficient raw data");
		}
		
		int offset = 0;	
        dwPreSignature = EndienConvertion.unsignedIntToLong(Arrays.copyOfRange(raw, offset,
                                                                               offset + 4),
                                                            ByteOrder.LITTLE_ENDIAN);
		//public long	dwPreSignature;	/* sync verification -- FR_PRESIG -- */
		offset += 4;

        RecordType = raw[offset];
		//public byte	RecordType;		/* Identifies following record type */
		offset += 1;

        Unused = raw[offset];
		//public byte	Unused;			/* for ??? in future */
		offset += 1;

        RecordLength = EndienConvertion.unsignedShortToInt(Arrays.copyOfRange(raw, offset,
                                                                              offset + 2),
                                                           ByteOrder.LITTLE_ENDIAN);
		//public int	RecordLength;	/* Total length of data rec in case different recs in future */
		offset += 2;

		// public long	Sequence;		/* Counter for verifying buffer reliability */
        Sequence = EndienConvertion.unsignedIntToLong(Arrays.copyOfRange(raw, offset, offset + 4),
                                                      ByteOrder.LITTLE_ENDIAN);
		offset += 4;

		//public char	[]Symbol;		/* Symbol for record */
        Symbol = Bytes.toString(EndienConvertion.reverseBytes(Arrays.copyOfRange(raw, offset,
                                                                                 offset + 48)))
                      .trim();
		offset += 48;

		// public TickDataType	TickData;
        TickData.fromBytes(Arrays.copyOfRange(raw, offset, offset + TickDataType.RECORD_SIZE));
		offset += TickDataType.RECORD_SIZE;

        dwPostSignature = EndienConvertion.unsignedIntToLong(Arrays.copyOfRange(raw, offset,
                                                                                offset + 4),
                                                             ByteOrder.LITTLE_ENDIAN);
		//public long	dwPostSignature;	/* sync verification -- FR_POSTSIG -- */	
		offset += 4;
	}

    public byte[] toBytes() {
        byte[] objInArray = new byte[RECORD_SIZE];

        int offset = 0;
        System.arraycopy(Bytes.toBytes(dwPreSignature), 4, objInArray, offset, 4);
        offset += 4;

        objInArray[offset] = RecordType;
        offset++;

        objInArray[offset] = Unused;
        offset++;

        System.arraycopy(Bytes.toBytes(RecordLength), 2, objInArray, offset, 2);
        offset += 2;

        System.arraycopy(Bytes.toBytes(Sequence), 4, objInArray, offset, 4);
        offset += 4;

        System.arraycopy(Bytes.toBytes(Symbol), 0, objInArray, offset, 48);
        offset += 48;

        System.arraycopy(TickData.toBytes(), 0, objInArray, offset, TickDataType.RECORD_SIZE);
        offset += TickDataType.RECORD_SIZE;

        System.arraycopy(Bytes.toBytes(dwPostSignature), 4, objInArray, offset, 4);
        offset += 4;

        return objInArray;

    }
}
