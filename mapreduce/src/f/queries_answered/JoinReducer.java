package f.queries_answered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer  extends Reducer<Text, Text, Text, NullWritable> {


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		List<Text> l = new ArrayList<>();
		boolean inTitle = false;
		for (Text value : values) {
			if (value.toString().equals("stop"))
				return;
			else if (value.toString().equals("wiki")) {
				inTitle = true;
			}
			else
				l.add(new Text(value.toString()));
		}
		if (inTitle)
			for (Text v: l) {
				context.write(v, NullWritable.get());
			}
	}
}
