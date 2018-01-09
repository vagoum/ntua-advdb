package f.queries_answered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueriesReducer  extends Reducer<Text, LongWritable, Text, LongWritable> {


	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		List<LongWritable> l = new ArrayList<>();
		
		for (LongWritable value : values) {
			if (value.get() == 0)
				return;
			l.add(new LongWritable(value.get()));
		}
		for (LongWritable v: l) {
			context.write(key,v);
		}
	}
}
