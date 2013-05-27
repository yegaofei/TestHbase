package com.insigma.tickserver;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestRegionInfo {

	private static final long SAMPLE_SIZE = RegionInfo.REGION_COUNT * RegionInfo.MAX_ROW_PER_REGION ;
	
	@Test
	public void testKeyGenerate() {
		
		long[] resideCount = new long[RegionInfo.REGION_COUNT];
		long total = 0;
		
		for(long i = 0; i < SAMPLE_SIZE; i++){
			byte[] key = RegionInfo.keyGenerate(i);
			
			long keyVar = Bytes.toLong(key);
			
			if(keyVar > SAMPLE_SIZE){
				Assert.fail("The key value is greater than the sample size");
			}
			int mod = (int) (keyVar / (SAMPLE_SIZE / RegionInfo.REGION_COUNT));
			resideCount[mod]++;
			}
					
		for(long l : resideCount){
			System.out.println(l);
			total += l;
		}
		System.out.println("Total is " + total);
		Assert.assertEquals(total, SAMPLE_SIZE);
		
	}

}
