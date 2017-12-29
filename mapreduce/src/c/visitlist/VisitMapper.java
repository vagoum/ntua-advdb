package c.visitlist;

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

public class VisitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String input = value.toString();
		String[] arr = input.split("\t");
		if (arr.length > 4 && input.contains("http")){
			context.write(new Text(arr[5]), new IntWritable(1));
		}

	}
}