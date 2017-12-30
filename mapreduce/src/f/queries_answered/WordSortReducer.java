package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordSortReducer  extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
			InterruptedException {
		// Go through all values to sum up card values for a card suit

		context.write(key, new LongWritable(-1));
	}
	

}
