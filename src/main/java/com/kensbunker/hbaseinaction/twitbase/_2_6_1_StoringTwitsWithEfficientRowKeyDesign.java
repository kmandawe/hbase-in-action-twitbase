package com.kensbunker.hbaseinaction.twitbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.Md5Crypt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hdfs.util.MD5FileUtils;

import java.io.IOException;

public class _2_6_1_StoringTwitsWithEfficientRowKeyDesign {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("twits"))) {

      // Make variable-length rowkey to fixed-length
      int longLength = Long.SIZE / 8;
      byte[] userHash = DigestUtils.md5("TheRealMT");
      byte[] timestamp = Bytes.toBytes(-1 * 1329088818321L);
      byte[] rowkey = new byte[DigestUtils.getMd5Digest().getDigestLength() + longLength];
      int offset = 0;
      offset = Bytes.putBytes(rowkey, offset, userHash, 0, userHash.length);
      Bytes.putBytes(rowkey, offset, timestamp, 0, timestamp.length);

      Put p = new Put(rowkey);
      p.addColumn(Bytes.toBytes("twits"), Bytes.toBytes("user"), Bytes.toBytes("TheRealMT"));
      p.addColumn(Bytes.toBytes("twits"), Bytes.toBytes("twit"), Bytes.toBytes("Hello, TwitBase!"));
      table.put(p);
    }
  }
}
