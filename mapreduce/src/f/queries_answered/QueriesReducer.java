package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import e.histogram.CustomCounter;

public class QueriesReducer  extends Reducer<Text, LongWritable, Text, LongWritable> {

	private Long counter = 0L;

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		for (LongWritable value : values) {
			if (value.get() == 0)
				return;
			context.write(key, value);
		}

	}
	/*
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());
		counter = currentJob.getCounters().findCounter(CustomCounter.Total).getValue();
	}
	*/
}
