package com.kensbunker.hbaseinaction.twitbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class _2_7_0_AtomicOperations {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("zookeeper.recovery.retry", 0);
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("users"))) {

      long ret =
          table.incrementColumnValue(
              Bytes.toBytes("TheRealMT"), Bytes.toBytes("info"), Bytes.toBytes("tweet_count"), 1L);

      System.out.println("Current count: " + ret);
    }
  }
}
