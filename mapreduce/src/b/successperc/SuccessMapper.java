package b.successperc;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SuccessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public static final String success = "Success";
	public static final String fail = "Fail";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String input = value.toString();
		if (input.contains("http")) {
			context.write(new Text(success), new IntWritable(1));
		}
		else {
			context.write(new Text(fail), new IntWritable(1));	
		}
	}
}