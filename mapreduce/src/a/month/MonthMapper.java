package a.month;

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

public class MonthMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			int month = cal.get(Calendar.MONTH);
			String smonth = Integer.toString(month);
			if (month < 10)
				smonth = "0" + smonth;
			int year = cal.get(Calendar.YEAR);
			String s = year + "-" + smonth;
			context.write(new Text(s), new IntWritable(1));
		} 
	}
}