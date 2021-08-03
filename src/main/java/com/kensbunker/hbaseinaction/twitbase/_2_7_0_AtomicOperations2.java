package com.kensbunker.hbaseinaction.twitbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class _2_7_0_AtomicOperations2 {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("zookeeper.recovery.retry", 0);
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("users"))) {

      Get g = new Get(Bytes.toBytes("TheRealMT"));
      Result r = table.get(g);
      long curVal =
          Bytes.toLong(
                  CellUtil.cloneValue(r.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("tweet_count"))));
      long incVal = curVal + 1;
      Put p = new Put(Bytes.toBytes("TheRealMT"));
      p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("tweet_count"), Bytes.toBytes(incVal));

      // checkAndPut
      table
          .checkAndMutate(Bytes.toBytes("TheRealMT"), Bytes.toBytes("info"))
          .qualifier(Bytes.toBytes("tweet_count"))
          .ifEquals(Bytes.toBytes(curVal))
          .thenPut(p);
    }
  }
}
