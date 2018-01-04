package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinDupReducer extends
		Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

	@Override
	public void reduce(LongWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		context.write(key, NullWritable.get());
	}

}