package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StopListMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString().toLowerCase();

		String words[] = inputLine.split("_");
		for (int i = 0; i < words.length; i++)
			if (!words[i].isEmpty())
				context.write(new Text(words[i]), new LongWritable(0));
	}
}