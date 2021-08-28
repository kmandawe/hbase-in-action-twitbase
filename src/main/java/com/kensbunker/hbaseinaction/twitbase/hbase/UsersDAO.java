package com.kensbunker.hbaseinaction.twitbase.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UsersDAO {
  public static final String TABLE_NAME = ("users");
  public static final byte[] INFO_FAM = Bytes.toBytes("info");
  public static final byte[] USER_COL = Bytes.toBytes("user");
  public static final byte[] NAME_COL = Bytes.toBytes("name");
  public static final byte[] EMAIL_COL = Bytes.toBytes("email");
  public static final byte[] PASS_COL = Bytes.toBytes("password");
  public static final byte[] TWEETS_COL = Bytes.toBytes("tweet_count");

  private Connection connection;

  public UsersDAO(Connection connection) {
    this.connection = connection;
  }

  private static Get mkGet(String user) {
    Get g = new Get(Bytes.toBytes(user));
    g.addFamily(INFO_FAM);
    return g;
  }

  private static Put mkPut(User u) {
    Put p = new Put(Bytes.toBytes(u.user));
    p.addColumn(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
    p.addColumn(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
    p.addColumn(INFO_FAM, EMAIL_COL, Bytes.toBytes(u.email));
    p.addColumn(INFO_FAM, PASS_COL, Bytes.toBytes(u.password));
    return p;
  }

  private static Delete mkDel(String user) {
    Delete d = new Delete(Bytes.toBytes(user));
    return d;
  }

  private static Scan mkScan() {
    Scan s = new Scan();
    s.addFamily(INFO_FAM);
    return s;
  }

  public void addUser(String user, String name, String email, String password) throws IOException {
    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      Put p = mkPut(new User(user, name, email, password));
      table.put(p);
    }
  }

  public com.kensbunker.hbaseinaction.twitbase.model.User getUser(String user) throws IOException {
    User u = null;
    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      Get g = mkGet(user);
      Result r = table.get(g);
      if (!r.isEmpty()) {
        u = new User(r);
      }
    }
    return u;
  }

  public void deleteUser(String user) throws IOException {
    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      Delete d = mkDel(user);
      table.delete(d);
    }
  }

  public List<com.kensbunker.hbaseinaction.twitbase.model.User> getUsers()
          throws IOException {
    List<com.kensbunker.hbaseinaction.twitbase.model.User> ret
            = new ArrayList<com.kensbunker.hbaseinaction.twitbase.model.User>();
    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      ResultScanner results = table.getScanner(mkScan());
      for(Result r : results) {
        ret.add(new User(r));
      }
    }
    return ret;
  }


  private static class User extends com.kensbunker.hbaseinaction.twitbase.model.User {
    private User(Result r) {
      this(
          r.getValue(INFO_FAM, USER_COL),
          r.getValue(INFO_FAM, NAME_COL),
          r.getValue(INFO_FAM, EMAIL_COL),
          r.getValue(INFO_FAM, PASS_COL),
          r.getValue(INFO_FAM, TWEETS_COL) == null
              ? Bytes.toBytes(0L)
              : r.getValue(INFO_FAM, TWEETS_COL));
    }

    private User(byte[] user, byte[] name, byte[] email, byte[] password, byte[] tweetCount) {
      this(
          Bytes.toString(user),
          Bytes.toString(name),
          Bytes.toString(email),
          Bytes.toString(password));
      this.tweetCount = Bytes.toLong(tweetCount);
    }

    private User(String user, String name, String email, String password) {
      this.user = user;
      this.name = name;
      this.email = email;
      this.password = password;
    }
  }
}
