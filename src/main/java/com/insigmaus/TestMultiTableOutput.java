package com.insigmaus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/** 
 * 
 * @author  Philip Ye [GYe@insigmaus.com]
 * @version V1.0  Create Time: Apr 24, 2013
 */

public class TestMultiTableOutput extends Configured implements Tool {

    private static final int SBAR_COUNT = 500000;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private static final String SBAR_TABLE_NAME = "bar4";

    private static final int TABLE_COUNT = 8;
    private static final String[] TABLE_NAMES = new String[TABLE_COUNT];
    static {
        for (int i = 0; i < TABLE_NAMES.length; i++) {
            String tableName = SBAR_TABLE_NAME + "_" + i;
            TABLE_NAMES[i] = tableName;
        }
    }

    private Configuration conf;

    private HTable[] tables;

    private boolean flushCommits;

    private boolean writeToWAL;

    public TestMultiTableOutput() {
        this.tables = new HTable[TABLE_NAMES.length];
        this.conf = HBaseConfiguration.create();

        this.flushCommits = true;
        this.writeToWAL = true;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("Start generating OutFile.csv");
        GenerateTextFile gtf = new GenerateTextFile();
        gtf.generateTextFile(SBAR_COUNT);
        System.out.println("End generating OutFile.csv");

        args = new String[] { "OutFile.csv" };
        try {
            int res = ToolRunner.run(new TestMultiTableOutput(), args);
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

    }

    private boolean tableExists(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(this.conf);

        return (admin.tableExists(tableName));
    }

    public void createTable(String table) throws IOException {
        byte[][] sBarFamilyNames = { FAMILY_NAME };
        createTable(table, sBarFamilyNames);
    }

    private void createTable(String tableName, byte[][] familyNames) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(this.conf);
        {
            boolean tableAvailable = admin.isTableAvailable(tableName);
            if (tableAvailable) {
                deleteTable(tableName);
            }
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for (byte[] familyName : familyNames) {
            HColumnDescriptor colDesc = new HColumnDescriptor(familyName);
            // colDesc.setCompressionType(Compression.Algorithm.LZO);
            tableDescriptor.addFamily(colDesc);
        }

        admin.createTable(tableDescriptor);
        boolean tableAvailable = admin.isTableAvailable(tableName);
        if (tableAvailable) {
            System.out.println("table created:" + tableName);
        }
    }

    private void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(this.conf);

        if (!admin.tableExists(tableName)) {
            return;
        }

        if (!admin.isTableDisabled(tableName)) {
            System.out.println("Disabling table..." + tableName);
            admin.disableTable(tableName);
        }

        System.out.println("Deleting table..." + tableName);
        admin.deleteTable(tableName);
    }

    void testSetup() throws IOException {
        for (int i = 0; i < TABLE_NAMES.length; i++) {
            this.tables[i] = new HTable(conf, Bytes.toBytes(TABLE_NAMES[i]));
            this.tables[i].setAutoFlush(false);
            // this.tables[i].setScannerCaching(30);
        }
    }

    void testTakedown() throws IOException {
        if (flushCommits) {
            for (int i = 0; i < TABLE_NAMES.length; i++) {
                this.tables[i].flushCommits();
            }
            // this.table.flushCommits();
        }
        for (int i = 0; i < TABLE_NAMES.length; i++) {
            this.tables[i].close();
        }
    }

    private static class Mapper extends
            org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        /**
         * The map method splits the csv file according to this structure
         * brand,model,size (e.g. Cadillac,Seville,Midsize) and output all data
         * using brand as key and the couple model,size as value.
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,
                                                                      InterruptedException {
            String[] valueSplitted = value.toString().split(",");
            if (valueSplitted.length == 9) {
                String symbol = valueSplitted[0];
                String column_time = valueSplitted[1];
                String column_bar_len = valueSplitted[2];
                String column_flags = valueSplitted[3];
                String column_open = valueSplitted[4];
                String column_high = valueSplitted[5];
                String column_low = valueSplitted[6];
                String column_last = valueSplitted[7];
                String column_volume = valueSplitted[8];

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 8; i++) {
                    sb.append(column_time).append(",");
                    sb.append(column_bar_len).append(",");
                    sb.append(column_flags).append(",");
                    sb.append(column_open).append(",");
                    sb.append(column_high).append(",");
                    sb.append(column_low).append(",");
                    sb.append(column_last).append(",");
                    sb.append(column_volume);
                }
                outKey.set(symbol);
                outValue.set(sb.toString());
                context.write(outKey, outValue);
            }
        }
    }

    private Job getMultiTableOutputJob(String name, Path inputFile) throws IOException {
        Job job = new Job(getConf(), name);
        job.setJarByClass(MultiTableOutputTutorial.class);
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputFile);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        job.setNumReduceTasks(20);
        job.setReducerClass(Reducer.class);

        return job;
    }

    private static class Reducer extends
            org.apache.hadoop.mapreduce.Reducer<Text, Text, ImmutableBytesWritable, Writable> {

        static final byte[] COLUMN_TIME = Bytes.toBytes("b");
        static final byte[] COLUMN_BAR_LEN = Bytes.toBytes("c");
        static final byte[] COLUMN_FLAGS = Bytes.toBytes("d");
        static final byte[] COLUMN_OPEN = Bytes.toBytes("e");
        static final byte[] COLUMN_HIGH = Bytes.toBytes("f");
        static final byte[] COLUMN_LOW = Bytes.toBytes("g");
        static final byte[] COLUMN_LAST = Bytes.toBytes("h");
        static final byte[] COLUMN_VOLUME = Bytes.toBytes("i");

        ImmutableBytesWritable[] putTables = new ImmutableBytesWritable[8];
        {
            for (int i = 0; i < TABLE_COUNT; i++) {
                ImmutableBytesWritable putTable = new ImmutableBytesWritable(
                    Bytes.toBytes(TABLE_NAMES[i]));
                putTables[i] = putTable;
            }
        }

        /**
         * The reduce method fill the TestCars table with all csv data, compute
         * some counters and save those counters into the TestBrandsSizes table.
         * So we use two different HBase table as output for the reduce method.
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                                                                               InterruptedException {
            String symbol = key.toString();

            // We are receiving all data.
            for (Text value : values) {
                String[] valueSplitted = value.toString().split(",");
                if (valueSplitted.length == 8) {
                    String column_time = valueSplitted[0];
                    String column_bar_len = valueSplitted[1];
                    String column_flags = valueSplitted[2];
                    String column_open = valueSplitted[3];
                    String column_high = valueSplitted[4];
                    String column_low = valueSplitted[5];
                    String column_last = valueSplitted[6];
                    String column_volume = valueSplitted[7];

                    // Fill the tables
                    byte[] putKey = Bytes.toBytes(symbol);
                    byte[] putFamily = FAMILY_NAME;
                    Put put = new Put(putKey);

                    // qualifier COLUMN_TIME
                    byte[] putQualifier = COLUMN_TIME;
                    byte[] putValue = Bytes.toBytes(column_time);
                    put.add(putFamily, putQualifier, putValue);
                    context.write(putTables[0], put);

                    // qualifier COLUMN_BAR_LEN
                    put = new Put(putKey);
                    putValue = Bytes.toBytes(column_bar_len);
                    put.add(putFamily, COLUMN_BAR_LEN, putValue);
                    context.write(putTables[1], put);

                    // qualifier COLUMN_FLAGS
                    put = new Put(putKey);
                    putValue = Bytes.toBytes(column_flags);
                    put.add(putFamily, COLUMN_FLAGS, putValue);
                    context.write(putTables[2], put);

                    put = new Put(putKey);
                    putValue = Bytes.toBytes(column_open);
                    put.add(putFamily, COLUMN_OPEN, putValue);
                    context.write(putTables[3], put);

                    put = new Put(putKey);
                    putValue = Bytes.toBytes(column_high);
                    put.add(putFamily, COLUMN_HIGH, putValue);
                    context.write(putTables[4], put);

                    put = new Put(putKey);
                    putValue = Bytes.toBytes(column_low);
                    put.add(putFamily, COLUMN_LOW, putValue);
                    context.write(putTables[5], put);

                    put = new Put(putKey);
                    putValue = Bytes.toBytes(column_last);
                    put.add(putFamily, COLUMN_LAST, putValue);
                    context.write(putTables[6], put);

                    put = new Put(putKey);
                    putValue = Bytes.toBytes(column_volume);
                    put.add(putFamily, COLUMN_VOLUME, putValue);
                    context.write(putTables[7], put);

                }
            }
        }
    }

    public int run(String[] arg0) throws Exception {
        for (int i = 0; i < TABLE_NAMES.length; i++) {
            String tableName = TABLE_NAMES[i];
            if (!tableExists(tableName)) {
                createTable(tableName);
            }
        }
        testSetup();

        try {
            String name = "Test MultiTableOutputFormat ";
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(conf);
            Path inputFile = new Path(fs.makeQualified(new Path(arg0[0])).toUri().getPath());
            long startTime = System.currentTimeMillis();
            if (!getMultiTableOutputJob(name, inputFile).waitForCompletion(true))
                System.exit(-1);
            long elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println("TimeElapsed:" + elapsedTime + "ms, Process " + SBAR_COUNT + " rows");
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            System.exit(-1);
        }
        testTakedown();
        return 0;
    }

}


