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

public class _2_2_8_VersionedData {
  public static void main(String[] args) throws IOException {
    // Opening a connection
    // Create a connection to the cluster.
    Configuration conf = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("users"))) {
      Get g = new Get(Bytes.toBytes("TheRealMT"));
      g.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
      g.readAllVersions();
      Result r = table.get(g);
      // convert back from bytes
      List<Cell> cells = r.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("password"));
      System.out.println(cells.size());
      String currentPassword = Bytes.toString(CellUtil.cloneValue(cells.get(0)));
      System.out.println(currentPassword);
      System.out.println("Timestamp: " + cells.get(0).getTimestamp());
      String previousPassword = Bytes.toString(CellUtil.cloneValue(cells.get(1)));
      System.out.println(previousPassword);
      System.out.println("Timestamp: " + cells.get(1).getTimestamp());
    }
  }
}
