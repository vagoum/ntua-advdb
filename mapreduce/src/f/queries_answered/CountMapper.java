package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.getCounter(CustomCounter.Success).increment(1);
		if (context.getCounter(CustomCounter.Success).getValue() == 1)
			context.write(new Text("Dummy Key"), new LongWritable(1));
	}
}