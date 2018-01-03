package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueriesReducer  extends Reducer<Text, LongWritable, Text, LongWritable> {


	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		for (LongWritable value : values) {
			if (value.get() == 0)
				return;
			context.write(key, value);
		}
	}
}
