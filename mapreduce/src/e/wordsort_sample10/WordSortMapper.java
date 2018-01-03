package e.wordsort_sample10;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordSortMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString().toLowerCase();
		inputLine = inputLine.replaceAll("[^a-z'0-9_]+", "");
		String words[] = inputLine.split("_");
		for (int i = 0; i < words.length; i++)
			if (!words[i].isEmpty()) {
				context.write(new Text(words[i]), NullWritable.get());
			}
	}
}