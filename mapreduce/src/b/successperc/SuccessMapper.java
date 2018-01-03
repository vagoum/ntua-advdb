package b.successperc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import e.histogram.CustomCounter;

public class SuccessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public static final String success = "Success";
	public static final String fail = "Fail";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(CustomCounter.Total).increment(1);

		String input = value.toString();
		String[] arr = input.split("\t");
		if (arr.length > 4 && input.contains("http")) {
			context.write(new Text(success), new IntWritable(1));
		}
		else {
			context.write(new Text(fail), new IntWritable(1));	
		}
	}
}