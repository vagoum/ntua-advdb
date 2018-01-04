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

		for (IntWritable value : values) {
			if (value.get() == 0)
				return;
			sum += value.get();
		}

		context.write(new IntWritable(sum), key);
	}
}
