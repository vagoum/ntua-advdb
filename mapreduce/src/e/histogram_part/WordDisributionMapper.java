package e.histogram_part;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class WordDisributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	//private int count = 0;
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		  setup(context);
		  int count = 0;
		  while (context.nextKeyValue() && count++ < 50) {
			  map(context.getCurrentKey(), context.getCurrentValue(), context);
		  }
		  cleanup(context);
		}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString().toLowerCase();
		inputLine = inputLine.replaceAll("[^a-z_0-9-!@#$%^&=+\\\\|\\[{\\]};:'\",<>/]+", "");
		String words[] = inputLine.split("_");
		for (int i = 0; i < words.length; i++)
			if (!words[i].isEmpty()) {
				char startingChar = words[i].charAt(0);
				context.getCounter(CustomCounter.Total).increment(1);

				if (Character.isDigit(startingChar)) {
					context.write(new Text("Digit"), new IntWritable(1));
				}
				else if (Character.isLetter(startingChar)) {
					context.write(new Text(startingChar + ""), new IntWritable(1));
				}
				else {
					context.write(new Text("Symbol"), new IntWritable(1));				
				}
			}
	}
}