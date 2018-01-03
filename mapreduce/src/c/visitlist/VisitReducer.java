package c.visitlist;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VisitReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		int sum = 0;

		Set<Integer> set = new HashSet<>();
		
		for (IntWritable value : values) {
			if (!set.contains(value.get())) {
				sum++;
				set.add(value.get());
			}
		}

		if (sum > 10)
			context.write(key, new IntWritable(sum));
	}
}
