package com.kensbunker.hbaseinaction.twitbase;

import com.kensbunker.hbaseinaction.twitbase.hbase.TwitsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.Random;

public class _3_5_1_CountShakespeare {

  public static class Map extends TableMapper<Text, LongWritable> {
    private Random rand;

    public static enum Counters {
      ROWS,
      SHAKESPEAREAN
    }

    /** Determines if the message pertains to Shakespeare. */
    private boolean containsShakespeare(String msg) {
      return rand.nextBoolean();
    }

    @Override
    protected void setup(Context context) {
      rand = new Random(System.currentTimeMillis());
    }

    @Override
    protected void map(
        ImmutableBytesWritable key,
        Result result,
        Mapper<ImmutableBytesWritable, Result, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {

      byte[] b = result.getValue(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);
      String msg = Bytes.toString(b);
      if (msg != null && !msg.isEmpty()) {
        context.getCounter(Counters.ROWS).increment(1);
      }
      if (containsShakespeare(msg)) {
        context.getCounter(Counters.SHAKESPEAREAN).increment(1);
      }
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("zookeeper.recovery.retry", 0);
    Job job = new Job(conf, "TwitBase Shakespeare counter");
    job.setJarByClass(_3_5_1_CountShakespeare.class);

    Scan scan = new Scan();
    scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);
    TableMapReduceUtil.initTableMapperJob(
        Bytes.toString(TwitsDAO.TABLE_NAME),
        scan,
        Map.class,
        ImmutableBytesWritable.class,
        Result.class,
        job);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
