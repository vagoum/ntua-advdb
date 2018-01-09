package f.queries_answered;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class QueriesMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.getCounter(CustomCounter.Total).increment(1);
		Long qid = context.getCounter(CustomCounter.Total).getValue();

		String input = value.toString().toLowerCase();
		String[] arr = input.split("\t");
		if (arr.length >= 2) {
			String[] words = arr[1].split("\\s+");

			for (String word : words) {
				context.write(new Text(word), new LongWritable(qid));
			}
		}
	}
}