package d.commonwords;

import java.io.File;

import org.apache.hadoop.fs.FileUtil;

public class CommonWords {

	public static void main(String[] args) throws Exception {
		String input = null, output = null, stop = null;
		if (args.length == 3) {
			input = args[0];
			stop = args[1];
			output = args[2];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			System.exit(-1);
		}
		
		String intermediatePath = new String("./tmp");
		String arr[] = new String[3];
		arr[0] = input;
		arr[1] = stop;
		arr[2] = intermediatePath;
		
		ExtractWordsDriver extractWordsDriver = new ExtractWordsDriver();
		extractWordsDriver.run(arr);
		
		arr = new String[2];
		arr[0] = intermediatePath;
		arr[1] = output;
		SortDriver sortDriver = new SortDriver();
		sortDriver.run(arr);

		FileUtil.fullyDelete(new File(intermediatePath));
	}

}
