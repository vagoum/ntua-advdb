package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TitlesMapper extends
		Mapper<LongWritable, Text, Text, Text> {


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString().toLowerCase();
		String words[] = inputLine.split("_");
		for (int i = 0; i < words.length; i++)
			if (!words[i].isEmpty()) {
				context.write(new Text(words[i]), new Text("wiki"));
			}
	}
}