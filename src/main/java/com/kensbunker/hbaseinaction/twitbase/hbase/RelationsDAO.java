package com.kensbunker.hbaseinaction.twitbase.hbase;

import com.kensbunker.hbaseinaction.twitbase.utils.Md5Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RelationsDAO {

  // md5(id_from)md5(id_to) -> 'f':id_to=name_to
  // md5(id_from)md5(id_to) -> 'f':'to'=id_to, 'f':'from'=id_from

  public static final byte[] FOLLOWS_TABLE_NAME = Bytes.toBytes("follows");
  public static final byte[] FOLLOWED_TABLE_NAME = Bytes.toBytes("followedBy");
  public static final byte[] RELATION_FAM = Bytes.toBytes("f");
  public static final byte[] FROM = Bytes.toBytes("from");
  public static final byte[] TO = Bytes.toBytes("to");

  private static final int KEY_WIDTH = 2 * Md5Utils.MD5_LENGTH;

  private Connection connection;

  public RelationsDAO(Connection connection) {
    this.connection = connection;
  }

  public static byte[] mkRowKey(String a) {
    byte[] ahash = Md5Utils.md5sum(a);
    byte[] rowkey = new byte[KEY_WIDTH];

    Bytes.putBytes(rowkey, 0, ahash, 0, ahash.length);
    return rowkey;
  }

  public static byte[] mkRowKey(String a, String b) {
    byte[] ahash = Md5Utils.md5sum(a);
    byte[] bhash = Md5Utils.md5sum(b);
    byte[] rowkey = new byte[KEY_WIDTH];

    int offset = 0;
    offset = Bytes.putBytes(rowkey, offset, ahash, 0, ahash.length);
    Bytes.putBytes(rowkey, offset, bhash, 0, bhash.length);
    return rowkey;
  }

  public static byte[][] splitRowkey(byte[] rowkey) {
    byte[][] result = new byte[2][];

    result[0] = Arrays.copyOfRange(rowkey, 0, Md5Utils.MD5_LENGTH);
    result[1] = Arrays.copyOfRange(rowkey, Md5Utils.MD5_LENGTH, KEY_WIDTH);
    return result;
  }

  public void addFollows(String fromId, String toId) throws IOException {
    addRelation(FOLLOWS_TABLE_NAME, fromId, toId);
  }

  public void addFollowedBy(String fromId, String toId) throws IOException {
    addRelation(FOLLOWED_TABLE_NAME, fromId, toId);
  }

  public void addRelation(byte[] table, String fromId, String toId) throws IOException {

    try (Table t = connection.getTable(TableName.valueOf(table))) {
      Put p = new Put(mkRowKey(fromId, toId));
      p.addColumn(RELATION_FAM, FROM, Bytes.toBytes(fromId));
      p.addColumn(RELATION_FAM, TO, Bytes.toBytes(toId));
      t.put(p);
    }
  }

  public List<com.kensbunker.hbaseinaction.twitbase.model.Relation> listFollows(String fromId)
      throws IOException {
    return listRelations(FOLLOWS_TABLE_NAME, fromId);
  }

  public List<com.kensbunker.hbaseinaction.twitbase.model.Relation> listFollowedBy(String fromId)
      throws IOException {
    return listRelations(FOLLOWED_TABLE_NAME, fromId);
  }

  public List<com.kensbunker.hbaseinaction.twitbase.model.Relation> listRelations(
      byte[] table, String fromId) throws IOException {

    List<com.kensbunker.hbaseinaction.twitbase.model.Relation> ret;
    try (Table t = connection.getTable(TableName.valueOf(table))) {
      String rel = (Bytes.equals(table, FOLLOWS_TABLE_NAME)) ? "->" : "<-";

      byte[] startKey = mkRowKey(fromId);
      byte[] endKey = Arrays.copyOf(startKey, startKey.length);
      endKey[Md5Utils.MD5_LENGTH - 1]++;
      Scan scan = new Scan(startKey, endKey);
      scan.addColumn(RELATION_FAM, TO);
      scan.setMaxVersions(1);

      ResultScanner results = t.getScanner(scan);
      ret = new ArrayList<>();
      for (Result r : results) {
        String toId = Bytes.toString(r.getValue(RELATION_FAM, TO));
        ret.add(new Relation(rel, fromId, toId));
      }
    }
    return ret;
  }

  @SuppressWarnings("unused")
  public long followedByCountScan(String user) throws IOException {
    long sum = 0;
    try (Table followed = connection.getTable(TableName.valueOf(FOLLOWED_TABLE_NAME))) {
      final byte[] startKey = Md5Utils.md5sum(user);
      final byte[] endKey = Arrays.copyOf(startKey, startKey.length);
      endKey[endKey.length - 1]++;
      Scan scan = new Scan(startKey, endKey);
      scan.setMaxVersions(1);

      ResultScanner rs = followed.getScanner(scan);
      for (Result r : rs) {
        sum++;
      }
    }
    return sum;
  }

  //  public long followedByCount(final String userId) throws Throwable {
  //
  //    long sum = 0;
  //    try (Table followed = connection.getTable(TableName.valueOf(FOLLOWED_TABLE_NAME))) {
  //      final byte[] startKey = Md5Utils.md5sum(userId);
  //      final byte[] endKey = Arrays.copyOf(startKey, startKey.length);
  //      endKey[endKey.length - 1]++;
  //
  //      Batch.Call<RelationCountProtocol, Long> callable =
  //              new Batch.Call<RelationCountProtocol, Long>() {
  //                @Override
  //                public Long call(RelationCountProtocol instance) throws IOException {
  //                  return instance.followedByCount(userId);
  //                }
  //              };
  //
  //      Map<byte[], Long> results =
  //              followed.coprocessorExec(RelationCountProtocol.class, startKey, endKey, callable);
  //
  //      for (Map.Entry<byte[], Long> e : results.entrySet()) {
  //        sum += e.getValue().longValue();
  //      }
  //
  //    }
  //    return sum;
  //  }

  private static class Relation extends com.kensbunker.hbaseinaction.twitbase.model.Relation {

    private Relation(String relation, String from, String to) {
      this.relation = relation;
      this.from = from;
      this.to = to;
    }
  }
}
