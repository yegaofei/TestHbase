package com.insigmaus.util;




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


        // long regionId = 1000 % RegionInfo.REGION_COUNT;
        // System.out.println(regionId);
        //
        // System.out.println(Long.valueOf("a", 16));
        // System.out.println(Integer.toHexString('a'));
        // System.out.println(Integer.toHexString('f'));
        // System.out.println(Integer.toHexString('A'));
        // System.out.println(Integer.toHexString('F'));

        System.out.println(Long.valueOf("000bebb41fd7b28e", 16));

        String s1 = convertAsc2Hex("\\x00\\x00\\x00=\\xE2\\xEA)\\xDC");
        System.out.println(s1);
        Long l1 = Long.valueOf(s1, 16);
        System.out.println(l1);

        String s2 = convertAsc2Hex("\\x02\\xF8\\xB5\\x8Ew\\xCC\\xF2\\x1E");
        System.out.println(s2);
        Long l2 = Long.valueOf(s2, 16);
        System.out.println(l2);

    }

    private static String convertAsc2Hex(String asc) {
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

        return sb.toString();
    }


}


