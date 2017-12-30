package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString().toLowerCase();
		String words[] = inputLine.split("\\s+");
		Long lv = Long.parseLong(words[1]);
		context.write(new Text(words[0]), new LongWritable(lv));
	}
}