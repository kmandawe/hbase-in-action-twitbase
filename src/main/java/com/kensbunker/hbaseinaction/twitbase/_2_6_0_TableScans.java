package com.kensbunker.hbaseinaction.twitbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class _2_6_0_TableScans {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number",1);
    conf.setInt("zookeeper.recovery.retry",0);
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("users"))) {

      Scan s = new Scan();
      ResultScanner resultScanner = table.getScanner(s);
      resultScanner.forEach(
          result -> {
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("user"))));
          });

      System.out.println("WITH RANGE:");
      Scan s2 = new Scan(Bytes.toBytes("T"), Bytes.toBytes("U"));
      ResultScanner resultScanner2 = table.getScanner(s2);
      resultScanner2.forEach(
              result -> {
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("user"))));
              });
    }
  }
}
