package d.commonwords;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExtractWordsReducer  extends Reducer<Text, IntWritable, IntWritable, Text> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		int sum = 0;

		// Go through all values to sum up card values for a card suit
		for (IntWritable value : values) {
			sum += value.get();
		}

		context.write(new IntWritable(sum), key);
	}
}
