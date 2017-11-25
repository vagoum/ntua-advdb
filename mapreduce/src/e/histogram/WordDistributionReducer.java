package e.histogram;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class WordDistributionReducer  extends Reducer<Text, IntWritable, Text, FloatWritable> {

	private Long counter = 0L;
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		int sum = 0;
		Long c = context.getCounter(CustomCounter.Total).getValue();
		// Go through all values to sum up card values for a card suit
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new FloatWritable((float) (1.0*sum/counter)));
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());
		counter = currentJob.getCounters().findCounter(CustomCounter.Total).getValue();
	}
	
}
