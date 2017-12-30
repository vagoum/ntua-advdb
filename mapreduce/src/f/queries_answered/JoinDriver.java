package f.queries_answered;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinDriver extends Configured implements Tool {


	@Override
	public int run(String[] args) throws Exception {
		String titles, output, queries;
		if (args.length == 3) {
			titles = args[0];
			queries = args[1];
			output = args[2];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}


		Job job = new Job();
		job.setJarByClass(JoinDriver.class);
		job.setJobName(this.getClass().getName());
		MultipleInputs.addInputPath(job,new Path(titles),TextInputFormat.class,JoinMapper.class);
		MultipleInputs.addInputPath(job,new Path(queries),TextInputFormat.class,JoinMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		JoinDriver driver = new JoinDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}