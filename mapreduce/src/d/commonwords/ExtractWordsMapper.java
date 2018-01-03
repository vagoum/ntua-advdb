package d.commonwords;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExtractWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString().toLowerCase();
		// inputLine = inputLine.replaceAll("[^a-z_]+", "");
		String arr[] = inputLine.split("\t");
		if (arr.length > 1) {
		String words[] = arr[1].split("\\s+");
		for (int i = 0; i < words.length; i++)
			if (!words[i].isEmpty())
				context.write(new Text(words[i]), new IntWritable(1));
		}
	}
}