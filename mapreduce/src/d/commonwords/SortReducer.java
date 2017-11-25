package d.commonwords;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {

		for(Text txt: values) {
			context.write(txt, key);
		}
	}
}
