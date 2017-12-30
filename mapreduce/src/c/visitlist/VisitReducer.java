package c.visitlist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VisitReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		int sum = 0;

		List<IntWritable> sortedList = new ArrayList<>();
		for (IntWritable i : values) {
		    sortedList.add(i);
		}
		
		IntWritable tmp = new IntWritable(0);
		for (IntWritable value : sortedList) {
			if (tmp.get() != value.get())
				sum++;
			tmp.set(value.get());
		}

		if (sum > 10)
			context.write(key, new IntWritable(sum));
	}
}
