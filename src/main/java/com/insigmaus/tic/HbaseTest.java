package com.insigmaus.tic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: May 3, 2013
 */

public class HbaseTest {

    /**
     * @param args
     */
    public static void main(String[] args) {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.0.37.20");
        System.out.println(config);

        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor[] tables = admin.listTables();
            HTableDescriptor table = tables[0];
            String tableName = Bytes.toString(table.getName());
            System.out.println(tableName);
            admin.close();
        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
