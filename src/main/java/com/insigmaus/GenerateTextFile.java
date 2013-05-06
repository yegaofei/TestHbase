package com.insigmaus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;


/** 
 * 
 * @author  Philip Ye [GYe@insigmaus.com]
 * @version V1.0  Create Time: Apr 24, 2013
 */

public class GenerateTextFile {

    private static final long timestamp = 1312840920l;

    private static final String FILE_NAME = "OutFile.csv";

    public void generateTextFile(int rowCount) {
        PrintStream out = null;
        try {
            File f = new File(FILE_NAME);
            if (f.exists()) {
                f.delete();
            }

            out = new PrintStream(new FileOutputStream(FILE_NAME));

            for (int i = 0; i < rowCount; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append(SymbolLoader.randomSymbol()).append(",");
                sb.append(timestamp + i).append(",");
                sb.append(1).append(",");
                sb.append(0).append(",");
                sb.append(RandomUtil.getDouble()).append(",");
                sb.append(RandomUtil.getDouble()).append(",");
                sb.append(RandomUtil.getDouble()).append(",");
                sb.append(RandomUtil.getDouble()).append(",");
                sb.append(RandomUtil.getInt());

                out.println((sb.toString()));
            }
            out.flush();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    public static byte[] format(final long number) {
        return format(number, 10);
    }

    public static byte[] format(final long number, int charCount) {
        byte[] b = new byte[charCount];
        long d = Math.abs(number);
        for (int i = b.length - 1; i >= 0; i--) {
            b[i] = (byte) ((d % 10) + '0');
            d /= 10;
        }
        return b;
    }

    public static void main(String[] args) {
        GenerateTextFile gtf = new GenerateTextFile();

        gtf.generateTextFile(5000);
    }
}


