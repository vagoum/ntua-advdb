package a.days;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DaysMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString();
		String regex = "(\\d{4}-\\d{2}-\\d{2})";
		Matcher m = Pattern.compile(regex).matcher(inputLine);
		if (m.find()) {
		    Date date = null;
			try {
				date = new SimpleDateFormat("yyyy-MM-dd").parse(m.group(1));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String stringDate = new SimpleDateFormat("yyyy-MM-dd").format(date);
			context.write(new Text(stringDate), new IntWritable(1));
		} 
	}
}