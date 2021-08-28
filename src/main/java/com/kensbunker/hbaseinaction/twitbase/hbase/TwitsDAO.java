package com.kensbunker.hbaseinaction.twitbase.hbase;

import com.kensbunker.hbaseinaction.twitbase.utils.Md5Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TwitsDAO {

  public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
  public static final byte[] TWITS_FAM = Bytes.toBytes("twits");

  public static final byte[] USER_COL = Bytes.toBytes("user");
  public static final byte[] TWIT_COL = Bytes.toBytes("twit");
  private static final int longLength = 8; // bytes

  private static final Logger log = Logger.getLogger(TwitsDAO.class);

  private Connection connection;

  public TwitsDAO(Connection connection) {
    this.connection = connection;
  }

  private static byte[] mkRowKey(Twit t) {
    return mkRowKey(t.user, t.dt);
  }

  private static byte[] mkRowKey(String user, DateTime dt) {
    byte[] userHash = Md5Utils.md5sum(user);
    byte[] timestamp = Bytes.toBytes(-1 * dt.getMillis());
    byte[] rowKey = new byte[Md5Utils.MD5_LENGTH + longLength];

    int offset = 0;
    offset = Bytes.putBytes(rowKey, offset, userHash, 0, userHash.length);
    Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);
    return rowKey;
  }

  private static Put mkPut(Twit t) {
    Put p = new Put(mkRowKey(t));
    p.addColumn(TWITS_FAM, USER_COL, Bytes.toBytes(t.user));
    p.addColumn(TWITS_FAM, TWIT_COL, Bytes.toBytes(t.text));
    return p;
  }

  private static Get mkGet(String user, DateTime dt) {
    Get g = new Get(mkRowKey(user, dt));
    g.addColumn(TWITS_FAM, USER_COL);
    g.addColumn(TWITS_FAM, TWIT_COL);
    return g;
  }

  private static String to_str(byte[] xs) {
    StringBuilder sb = new StringBuilder(xs.length * 2);
    for (byte b : xs) {
      sb.append(b).append(" ");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  private static Scan mkScan(String user) {
    byte[] userHash = Md5Utils.md5sum(user);
    byte[] startRow = Bytes.padTail(userHash, longLength); // 212d...866f00...
    byte[] stopRow = Bytes.padTail(userHash, longLength);
    stopRow[Md5Utils.MD5_LENGTH - 1]++; // 212d...867000...

    log.debug("Scan starting at: '" + to_str(startRow) + "'");
    log.debug("Scan stopping at: '" + to_str(stopRow) + "'");

    Scan s = new Scan(startRow, stopRow);
    s.addColumn(TWITS_FAM, USER_COL);
    s.addColumn(TWITS_FAM, TWIT_COL);
    return s;
  }

  public void postTwit(String user, DateTime dt, String text) throws IOException {
    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      Put p = mkPut(new Twit(user, dt, text));
      table.put(p);
    }
  }

  public com.kensbunker.hbaseinaction.twitbase.model.Twit getTwit(String user, DateTime dt)
      throws IOException {

    Twit t;
    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      Get g = mkGet(user, dt);
      Result result = table.get(g);
      if (result.isEmpty()) return null;
      t = new Twit(result);
    }
    return t;
  }

  public List<com.kensbunker.hbaseinaction.twitbase.model.Twit> list(String user)
      throws IOException {

    List<com.kensbunker.hbaseinaction.twitbase.model.Twit> ret;
    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
      ResultScanner results = table.getScanner(mkScan(user));
      ret = new ArrayList<>();
      for (Result r : results) {
        ret.add(new Twit(r));
      }
    }
    return ret;
  }

  private static class Twit extends com.kensbunker.hbaseinaction.twitbase.model.Twit {

    private Twit(Result r) {
      this(
          r.getValue(TWITS_FAM, USER_COL),
          Arrays.copyOfRange(r.getRow(), Md5Utils.MD5_LENGTH, Md5Utils.MD5_LENGTH + longLength),
          r.getValue(TWITS_FAM, TWIT_COL));
    }

    private Twit(byte[] user, byte[] dt, byte[] text) {
      this(Bytes.toString(user), new DateTime(-1 * Bytes.toLong(dt)), Bytes.toString(text));
    }

    private Twit(String user, DateTime dt, String text) {
      this.user = user;
      this.dt = dt;
      this.text = text;
    }
  }
}
