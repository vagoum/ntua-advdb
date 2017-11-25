package e.wordsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class WordSortReducer  extends Reducer<Text, NullWritable, Text, NullWritable> {

	@Override
	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {
		// Go through all values to sum up card values for a card suit

		context.write(key, NullWritable.get());
	}
	

}
