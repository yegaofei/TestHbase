package com.insigmaus.util;

import com.insigma.tickserver.RegionInfo;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: May 27, 2013
 */

public class HbaseValueConverter {

    /**
     * @param args
     */
    public static void main(String[] args) {

        long regionId = 1000 % RegionInfo.REGION_COUNT;
        System.out.println(regionId);

        System.out.println(Long.valueOf("a", 16));
        System.out.println(Integer.toHexString('a'));
        System.out.println(Integer.toHexString('f'));
        System.out.println(Integer.toHexString('A'));
        System.out.println(Integer.toHexString('F'));

        System.out.println(convertAsc2Hex("\\x00\\x00\\x00\\x00\\x02\\xBC\\x84a"));
        System.out.println(convertAsc2Hex("\\x00\\x00\\x00\\x00\\x02\\x9C\\x9C\\xC4"));


    }

    private static Long convertAsc2Hex(String asc) {
        String[] strArray = asc.split("\\\\x");
        StringBuilder sb = new StringBuilder();
        for (String s : strArray) {
            char[] theCharsArray = s.toCharArray();
            for (int i = 0; i < theCharsArray.length; i++) {
                char c = theCharsArray[i];
                String hexString = Integer.toHexString(c);
                Long hexLong = Long.valueOf(hexString, 16);

                if ((hexLong >= 0x41 && hexLong <= 0x46) || (hexLong >= 0x61 && hexLong <= 0x66)
                        || (hexLong >= 0x30 && hexLong <= 0x39)) {
                    sb.append(c);
                } else {
                    sb.append(hexString);
                }
            }
        }

        return Long.valueOf(sb.toString(), 16);
    }


}


