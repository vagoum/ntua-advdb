package d.commonwords;

import java.io.File;

import org.apache.hadoop.fs.FileUtil;

public class CommonWords {

	public static void main(String[] args) throws Exception {
		String input = null, output = null;
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			System.exit(-1);
		}
		
		String intermediatePath = new String("./tmp");
		String arr[] = new String[2];
		arr[0] = input;
		arr[1] = intermediatePath;
		
		ExtractWordsDriver extractWordsDriver = new ExtractWordsDriver();
		extractWordsDriver.run(arr);
		
		
		arr[0] = intermediatePath;
		arr[1] = output;
		SortDriver sortDriver = new SortDriver();
		sortDriver.run(arr);

		FileUtil.fullyDelete(new File(intermediatePath));
	}

}
