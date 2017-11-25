package d.commonwords;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String input = value.toString();
			String words[] = input.split("\\s+");
			LongWritable l = new LongWritable(Long.parseLong(words[0]));
			context.write(l, new Text(words[1]));
	}
}
