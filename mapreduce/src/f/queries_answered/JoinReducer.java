package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, LongWritable, LongWritable, NullWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		
		for (LongWritable value : values) {
			if (value.get() == -1) {
				for (LongWritable v: values) {
					if (v.get() != -1) {
						context.write(v, NullWritable.get());
					}
				}
				return;
			}
		}

	}
	

}
