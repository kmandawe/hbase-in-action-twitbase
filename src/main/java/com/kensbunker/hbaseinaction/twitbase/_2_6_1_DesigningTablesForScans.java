package com.kensbunker.hbaseinaction.twitbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class _2_6_1_DesigningTablesForScans {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("zookeeper.recovery.retry", 0);
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin()) {
      TableDescriptorBuilder tableBuilder =
          TableDescriptorBuilder.newBuilder(TableName.valueOf("twits"));
      ColumnFamilyDescriptorBuilder columnFamilyBuilder =
          ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("twits"));
      columnFamilyBuilder.setMaxVersions(1);
      tableBuilder.setColumnFamily(columnFamilyBuilder.build());
      admin.createTable(tableBuilder.build());
    }
  }
}
