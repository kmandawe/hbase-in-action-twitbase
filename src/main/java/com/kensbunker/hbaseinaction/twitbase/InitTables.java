package com.kensbunker.hbaseinaction.twitbase;

import com.kensbunker.hbaseinaction.twitbase.hbase.RelationsDAO;
import com.kensbunker.hbaseinaction.twitbase.hbase.TwitsDAO;
import com.kensbunker.hbaseinaction.twitbase.hbase.UsersDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class InitTables {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("zookeeper.recovery.retry", 0);
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin()) {

      // first do no harm
      if (args.length > 0 && args[0].equalsIgnoreCase("-f")) {
        System.out.println("!!! dropping tables in...");
        for (int i = 5; i > 0; i--) {
          System.out.println(i);
          Thread.sleep(1000);
        }

        if (admin.tableExists(TableName.valueOf(UsersDAO.TABLE_NAME))) {
          System.out.printf("Deleting %s\n", UsersDAO.TABLE_NAME);
          if (admin.isTableEnabled(TableName.valueOf(UsersDAO.TABLE_NAME)))
            admin.disableTable(TableName.valueOf(UsersDAO.TABLE_NAME));
          admin.deleteTable(TableName.valueOf(UsersDAO.TABLE_NAME));
        }

        if (admin.tableExists(TableName.valueOf(TwitsDAO.TABLE_NAME))) {
          System.out.printf("Deleting %s\n", Bytes.toString(TwitsDAO.TABLE_NAME));
          if (admin.isTableEnabled(TableName.valueOf(TwitsDAO.TABLE_NAME)))
            admin.disableTable(TableName.valueOf(TwitsDAO.TABLE_NAME));
          admin.deleteTable(TableName.valueOf(TwitsDAO.TABLE_NAME));
        }

        if (admin.tableExists(TableName.valueOf(RelationsDAO.FOLLOWS_TABLE_NAME))) {
          System.out.printf("Deleting %s\n", Bytes.toString(RelationsDAO.FOLLOWS_TABLE_NAME));
          if (admin.isTableEnabled(TableName.valueOf(RelationsDAO.FOLLOWS_TABLE_NAME)))
            admin.disableTable(TableName.valueOf(RelationsDAO.FOLLOWS_TABLE_NAME));
          admin.deleteTable(TableName.valueOf(RelationsDAO.FOLLOWS_TABLE_NAME));
        }

        if (admin.tableExists(TableName.valueOf(RelationsDAO.FOLLOWED_TABLE_NAME))) {
          System.out.printf("Deleting %s\n", Bytes.toString(RelationsDAO.FOLLOWED_TABLE_NAME));
          if (admin.isTableEnabled(TableName.valueOf(RelationsDAO.FOLLOWED_TABLE_NAME)))
            admin.disableTable(TableName.valueOf(RelationsDAO.FOLLOWED_TABLE_NAME));
          admin.deleteTable(TableName.valueOf(RelationsDAO.FOLLOWED_TABLE_NAME));
        }
      }

      if (admin.tableExists(TableName.valueOf(UsersDAO.TABLE_NAME))) {
        System.out.println("User table already exists.");
      } else {
        System.out.println("Creating User table...");
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(UsersDAO.TABLE_NAME));
        HColumnDescriptor c = new HColumnDescriptor(UsersDAO.INFO_FAM);
        desc.addFamily(c);
        admin.createTable(desc);
        System.out.println("User table created.");
      }

      if (admin.tableExists(TableName.valueOf(TwitsDAO.TABLE_NAME))) {
        System.out.println("Twits table already exists.");
      } else {
        System.out.println("Creating Twits table...");
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TwitsDAO.TABLE_NAME));
        HColumnDescriptor c = new HColumnDescriptor(TwitsDAO.TWITS_FAM);
        c.setMaxVersions(1);
        desc.addFamily(c);
        admin.createTable(desc);
        System.out.println("Twits table created.");
      }

      if (admin.tableExists(TableName.valueOf(RelationsDAO.FOLLOWS_TABLE_NAME))) {
        System.out.println("Follows table already exists.");
      } else {
        System.out.println("Creating Follows table...");
        HTableDescriptor desc =
            new HTableDescriptor(TableName.valueOf(RelationsDAO.FOLLOWS_TABLE_NAME));
        HColumnDescriptor c = new HColumnDescriptor(RelationsDAO.RELATION_FAM);
        c.setMaxVersions(1);
        desc.addFamily(c);
        admin.createTable(desc);
        System.out.println("Follows table created.");
      }

      if (admin.tableExists(TableName.valueOf(RelationsDAO.FOLLOWED_TABLE_NAME))) {
        System.out.println("Followed table already exists.");
      } else {
        System.out.println("Creating Followed table...");
        HTableDescriptor desc =
            new HTableDescriptor(TableName.valueOf(RelationsDAO.FOLLOWED_TABLE_NAME));
        HColumnDescriptor c = new HColumnDescriptor(RelationsDAO.RELATION_FAM);
        c.setMaxVersions(1);
        desc.addFamily(c);
        admin.createTable(desc);
        System.out.println("Followed table created.");
      }
    }
  }
}
