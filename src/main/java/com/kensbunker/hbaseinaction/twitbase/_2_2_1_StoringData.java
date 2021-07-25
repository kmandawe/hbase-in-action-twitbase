package com.kensbunker.hbaseinaction.twitbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class _2_2_1_StoringData {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("users"))) {

      Put p = new Put(Bytes.toBytes("TheRealMT"));
      p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Mark Twain"));
      p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes("samuel@clemens.org"));
      p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes("Langhorne"));

      table.put(p);
    }
  }
}
