package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinDupMapper extends
		Mapper<LongWritable, Text, LongWritable, NullWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

			context.write(new LongWritable(Long.parseLong(value.toString())), NullWritable.get());
	}
}