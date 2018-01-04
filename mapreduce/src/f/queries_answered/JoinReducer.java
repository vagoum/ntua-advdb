package f.queries_answered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends
		Reducer<Text, LongWritable, LongWritable, NullWritable> {



	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		List<LongWritable> l = new ArrayList<>();
		boolean flag = false;

		for (LongWritable value : values) {
			if (value.get() == -1) {
				flag = true;
			} else {
				l.add(new LongWritable(value.get()));
			}
		}

		if (flag) {
			for (LongWritable lgw : l) {
				context.write(lgw, NullWritable.get());
			}
		}
	}
}
