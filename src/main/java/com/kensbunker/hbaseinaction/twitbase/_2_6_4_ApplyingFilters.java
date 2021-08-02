package com.kensbunker.hbaseinaction.twitbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

public class _2_6_4_ApplyingFilters {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("zookeeper.recovery.retry", 0);
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("twits"))) {

      Scan s = new Scan();
      s.addColumn(Bytes.toBytes("twits"), Bytes.toBytes("twit"));
      Filter f = new ValueFilter(CompareOperator.EQUAL, new RegexStringComparator(".*TwitBase.*"));
      s.setFilter(f);
      ResultScanner rs = table.getScanner(s);


      for (Result r : rs) {
        // extract the twit
        byte[] twitBytes = r.getValue(Bytes.toBytes("twits"), Bytes.toBytes("twit"));
        String message = Bytes.toString(twitBytes);
        System.out.println("Twit: " + message);

      }
    }
  }
}
