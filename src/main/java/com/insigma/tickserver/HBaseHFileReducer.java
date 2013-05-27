package com.insigma.tickserver;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: May 24, 2013
 */

public class HBaseHFileReducer extends
        Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, KeyValue> {

    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
                                                                                             throws IOException,
                                                                                             InterruptedException {
        String value = "";
        while (values.iterator().hasNext()) {
            value = values.iterator().next().toString();
            if (value != null && !"".equals(value)) {
                KeyValue kv = createKeyValue(value.toString());
                if (kv != null)
                    context.write(key, kv);
            }
        }
    }

    private KeyValue createKeyValue(String str) {
        String[] strs = str.split(":");
        if (strs.length < 4)
            return null;
        String row = strs[0];
        String family = strs[1];
        String qualifier = strs[2];
        String value = strs[3];
        return new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier),
            System.currentTimeMillis(), Bytes.toBytes(value));
    }
}


