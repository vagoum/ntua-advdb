package e.wordsort_sample10;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordSortReducer  extends Reducer<Text, NullWritable, Text, NullWritable> {

	@Override
	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {

		context.write(key, NullWritable.get());
	}
	

}