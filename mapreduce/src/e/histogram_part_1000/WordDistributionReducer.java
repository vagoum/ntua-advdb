package e.histogram_part_1000;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class WordDistributionReducer  extends Reducer<Text, IntWritable, Text, Text> {

	private Long counter = 0L;
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		int sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new Text(String.format("%4.2f", 100.0*sum/counter)+"%"));
	}
	
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());
		counter = currentJob.getCounters().findCounter(CustomCounter.Total).getValue();
	}
}
