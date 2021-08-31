package com.kensbunker.hbaseinaction.twitbase.utils;

import com.kensbunker.hbaseinaction.twitbase.hbase.TwitsDAO;
import com.kensbunker.hbaseinaction.twitbase.hbase.UsersDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class _3_5_2_HamletTagger {
  public static class Map extends TableMapper<ImmutableBytesWritable, Put> {
    public static enum Counters {
      HAMLET_TAGS
    }

    private Random rand;

    private boolean mentionsHamlet(String msg) {
      return rand.nextBoolean();
    }

    @Override
    protected void setup(Context context) {
      rand = new Random(System.currentTimeMillis());
    }

    @Override
    protected void map(ImmutableBytesWritable rowKey, Result result, Context context)
        throws IOException, InterruptedException {

      byte[] b = result.getValue(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);
      String msg = Bytes.toString(b);
      b = result.getValue(TwitsDAO.TWITS_FAM, TwitsDAO.USER_COL);
      String user = Bytes.toString(b);
      if (mentionsHamlet(msg)) {
        Put p = UsersDAO.mkPut(user, UsersDAO.INFO_FAM, UsersDAO.HAMLET_COL, Bytes.toBytes(true));
        ImmutableBytesWritable outkey = new ImmutableBytesWritable(p.getRow());
        try {
          context.write(outkey, p);
          context.getCounter(Counters.HAMLET_TAGS).increment(1);
        } catch (Exception e) {
          // gulp!
        }
      }
    }
  }

  public static class Reduce
      extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable rowKey, Iterable<Put> values, Context context)
        throws IOException, InterruptedException {
      Iterator<Put> i = values.iterator();
      if (i.hasNext()) {
        try {
          context.write(rowKey, i.next());
        } catch (Exception e) {
          // gulp!
        }
      }
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = HBaseConfiguration.create();
    Job job = new Job(conf, "TwitBase Hamlet Tagger");
    job.setJarByClass(_3_5_2_HamletTagger.class);

    Scan scan = new Scan();
    scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.USER_COL);
    scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);
    TableMapReduceUtil.initTableMapperJob(
        Bytes.toString(TwitsDAO.TABLE_NAME),
        scan,
        Map.class,
        ImmutableBytesWritable.class,
        Put.class,
        job);
    TableMapReduceUtil.initTableReducerJob(UsersDAO.TABLE_NAME, IdentityTableReducer.class, job);

    job.setNumReduceTasks(0);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
