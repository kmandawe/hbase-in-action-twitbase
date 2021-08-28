package com.kensbunker.hbaseinaction.twitbase;

import com.kensbunker.hbaseinaction.twitbase.hbase.UsersDAO;
import com.kensbunker.hbaseinaction.twitbase.model.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class UsersTool {

  public static final String USAGE =
      "UsersTool action ...\n"
          + " help - print this message and exit.\n"
          + " add user name email password"
          + "- add a new user.\n"
          + " get user - retrieve a specific user.\n"
          + " list - list all installed users.\n";

  public static void main(String[] args) throws IOException {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(USAGE);
      System.exit(0);
    }

    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("zookeeper.recovery.retry", 0);
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      UsersDAO dao = new UsersDAO(connection);

      if ("get".equals(args[0])) {
        System.out.println("Getting user " + args[1]);
        User u = dao.getUser(args[1]);
        System.out.println(u);
      }

      if ("add".equals(args[0])) {
        System.out.println("Adding user...");
        dao.addUser(args[1], args[2], args[3], args[4]);
        User u = dao.getUser(args[1]);
        System.out.println("Successfully added user " + u);
      }

      if ("list".equals(args[0])) {
        for (User u : dao.getUsers()) {
          System.out.println(u);
        }
      }
    }
  }
}
