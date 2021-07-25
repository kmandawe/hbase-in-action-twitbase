package com.kensbunker.hbaseinaction.twitbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class _2_2_6_DeletingData {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("users"))) {
      Delete d = new Delete(Bytes.toBytes("TheRealMT"));
      d.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"));
      table.delete(d);
    }
  }
}
