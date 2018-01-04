package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer   extends Reducer<Text, LongWritable, Text, Text> {

	private Long counter = 0L;
	private Long successful = 0L;
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
			InterruptedException {
		Double tmp = 100.0*successful/counter;
		
		context.write(new Text("Success: "), new Text(String.format("%4.2f", tmp)+"%"));
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());
		successful = currentJob.getCounters().findCounter(CustomCounter.Success).getValue();
		counter = context.getConfiguration().getLong(CustomCounter.Total.toString(), 0);
	}
}
