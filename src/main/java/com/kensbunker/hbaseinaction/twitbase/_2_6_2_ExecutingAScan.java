package com.kensbunker.hbaseinaction.twitbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

public class _2_6_2_ExecutingAScan {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("zookeeper.recovery.retry", 0);
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("twits"))) {

      // Make variable-length rowkey to fixed-length
      int longLength = Long.SIZE / 8;
      int md5Length = DigestUtils.getMd5Digest().getDigestLength();
      byte[] userHash = DigestUtils.md5("TheRealMT");
      byte[] startRow = Bytes.padTail(userHash, longLength);
      byte[] stopRow = Bytes.padTail(userHash, longLength);
      stopRow[md5Length - 1]++;

      Scan s = new Scan().withStartRow(startRow).withStopRow(stopRow);
      ResultScanner rs = table.getScanner(s);

      for (Result r : rs) {
        // extract the username
        byte[] userBytes = r.getValue(Bytes.toBytes("twits"), Bytes.toBytes("user"));
        String user = Bytes.toString(userBytes);
        System.out.println("User: " + user);

        // extract the twit
        byte[] twitBytes = r.getValue(Bytes.toBytes("twits"), Bytes.toBytes("twit"));
        String message = Bytes.toString(twitBytes);
        System.out.println("Twit: " + message);

        // extract the timestamp
        byte[] dtBytes = Arrays.copyOfRange(r.getRow(), md5Length, md5Length + longLength);
        LocalDateTime dt =
            LocalDateTime.ofInstant(
                Instant.ofEpochMilli(-1 * Bytes.toLong(dtBytes)), ZoneId.systemDefault());

        System.out.println("DateTime: " + dt);
      }
    }
  }
}
