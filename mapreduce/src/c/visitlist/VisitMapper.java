package c.visitlist;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VisitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String input = value.toString();
		String[] arr = input.split("\t");
		if (arr.length > 4 && input.contains("http")){
			try {
				int id = Integer.parseInt(arr[0]);
				context.write(new Text(arr[4]), new IntWritable(id));
			}
			catch (IndexOutOfBoundsException e) {}
		}

	}
}