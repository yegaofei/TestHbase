package com.insigma.tickserver;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * PRICEDATA
 * 
 * @author gpan
 *
 */
public class PriceDataType {
	public double value;
	public long size;
	public long ucumvolume;
	public long flags;
	public byte []qualifiers;
	public byte []volqualifiers;
	public byte []exchange;
	
	public static int TICKDATA_QUAL_SIZE = 4;
	public static int TICKDATA_EXG_SIZE = 4;
	public static int TICKDATA_VOLQUAL_SIZE = 2;	
	
	public static int RECORD_SIZE = 30;
	
	public PriceDataType() {
		value = 0.0;
		size = 0;
		ucumvolume = 0;
		flags = 0;
		qualifiers = new byte[TICKDATA_QUAL_SIZE];
		volqualifiers = new byte[TICKDATA_VOLQUAL_SIZE];
		exchange = new byte[TICKDATA_EXG_SIZE];
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
		
		//double	value;
		//value = Bytes.toDouble(Arrays.copyOfRange(raw, offset, offset + 8 + 1));
        value = ByteBuffer.wrap(Arrays.copyOfRange(raw, offset, offset + 8)).getDouble();
		offset += 8;
		// ULONG32 size;	// maybe change to double later on.... (some sizes go to double size..or float)
        size = Bytes.toInt(Arrays.copyOfRange(raw, offset, offset + 4));
		offset += 4;
		//ULONG32 ucumvolume; // cumulative volume of the ticks... valid for trades only...
        ucumvolume = Bytes.toInt(Arrays.copyOfRange(raw, offset, offset + 4));
		offset += 4;
		//DWORD32	flags;	// flags for tickserver (translated to current tickserver flags)
        flags = Bytes.toInt(Arrays.copyOfRange(raw, offset, offset + 4));
		offset += 4;
		//BYTE	qualifiers[TICKDATA_QUAL_SIZE];
        qualifiers = Arrays.copyOfRange(raw, offset, offset + TICKDATA_QUAL_SIZE);
		offset += TICKDATA_QUAL_SIZE;
		//BYTE	volqualifiers[TICKDATA_VOLQUAL_SIZE];
        volqualifiers = Arrays.copyOfRange(raw, offset, offset + TICKDATA_VOLQUAL_SIZE);
		offset += TICKDATA_VOLQUAL_SIZE;
		//BYTE	exchange[TICKDATA_EXG_SIZE];
        exchange = Arrays.copyOfRange(raw, offset, offset + TICKDATA_EXG_SIZE);
		offset += TICKDATA_EXG_SIZE;
	}

    public byte[] toBytes() {
        byte[] objInArray = new byte[RECORD_SIZE];

        int offset = 0;
        System.arraycopy(Bytes.toBytes(value), 0, objInArray, offset, 8);
        offset += 8;

        System.arraycopy(Bytes.toBytes(size), 4, objInArray, offset, 4);
        offset += 4;

        System.arraycopy(Bytes.toBytes(ucumvolume), 4, objInArray, offset, 4);
        offset += 4;

        System.arraycopy(Bytes.toBytes(flags), 4, objInArray, offset, 4);
        offset += 4;

        System.arraycopy(qualifiers, 0, objInArray, offset, TICKDATA_QUAL_SIZE);
        offset += TICKDATA_QUAL_SIZE;

        System.arraycopy(volqualifiers, 0, objInArray, offset, TICKDATA_VOLQUAL_SIZE);
        offset += TICKDATA_VOLQUAL_SIZE;

        System.arraycopy(exchange, 0, objInArray, offset, TICKDATA_EXG_SIZE);
        offset += TICKDATA_EXG_SIZE;

        return objInArray;
    }

    public boolean equals(Object obj) {
        if (obj != null && obj instanceof PriceDataType) {
            PriceDataType pdt2 = (PriceDataType) obj;
            if (!Bytes.equals(pdt2.exchange, this.exchange)) {
                return false;
            }

            if (pdt2.flags != this.flags) {
                return false;
            }

            if (!Bytes.equals(pdt2.qualifiers, this.qualifiers)) {
                return false;
            }

            if (pdt2.size != this.size) {
                return false;
            }

            if (pdt2.ucumvolume != this.ucumvolume) {
                return false;
            }

            if (pdt2.value != this.value) {
                return false;
            }

            if (!Bytes.equals(pdt2.volqualifiers, this.volqualifiers)) {
                return false;
            }

            return true;
        }
        return false;
    }
}
