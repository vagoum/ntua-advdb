package f.queries_answered;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountDriver  extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input, output;
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}
		Job job = new Job();
		job.setJarByClass(WordSortDriver.class);
		job.setJobName(this.getClass().getName());
		
		
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.getConfiguration().setLong(CustomCounter.Total.toString(), QueriesAnswered.queryCounter.getValue());
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		WordSortDriver driver = new WordSortDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}