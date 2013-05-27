package com.insigma.tickserver;

import org.apache.hadoop.hbase.util.Bytes;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: May 27, 2013
 */

public class RegionInfo {

    public static final int REGION_COUNT = 24;

    public static final long MAX_ROW_PER_REGION = 2000000;
    
    private static long[] currentIndexEachRegion = new long[REGION_COUNT];
    
    static{
        for(int i = 0; i < REGION_COUNT; i++){
            currentIndexEachRegion[i] = 0L;
        }
    }

    private static long getAndUpdateCurrentIndex(int regionId) {
        long currentIndxe = currentIndexEachRegion[regionId];
        currentIndexEachRegion[regionId] = currentIndxe + 1;
        return currentIndxe;
    }

    public static byte[] keyGenerate(long keySeed) {
        long regionId = keySeed % RegionInfo.REGION_COUNT;
        
        long currentIndxe = getAndUpdateCurrentIndex((int) regionId);
        
        long regionBase = regionId * (long) RegionInfo.MAX_ROW_PER_REGION;
        
        return Bytes.toBytes(regionBase + currentIndxe);
    }

}


