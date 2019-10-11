package assignment1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.log4j.BasicConfigurator;

/**
 * 
 * This class solves the problem posed for Assignment1 
 * Name: Nan Li 
 * ID: Z5182060
 *
 */
public class Assignment1 {

	/**
	 * read the files in the dictionary, and seperate it in to ngram
	 * INPUT: FILE.toString();
	 * OUPUT: ngram		1	filename
	 */
	public static class ToNgramMapper extends Mapper<Object, Text, Text, IntTextWritable> {
		
		IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private int nnum;
		private Text filename = new Text();
		IntTextWritable outresult = new IntTextWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			nnum = Integer.valueOf(context.getConfiguration().get("ngram"));
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			filename.set(fileSplit.getPath().getName());
			
			String LastTokens = "";
			String ngram = "";
			String currentToken = "";
			
			//if the string is small than ngram, generate nothing
			if(itr.countTokens() >= nnum) {
				//First we generate the first ngram
				for (int i = 0; i < nnum; i++) {
					currentToken = itr.nextToken();
					ngram = ngram + currentToken + " ";
					if (i != 0) {
						LastTokens = LastTokens + currentToken + " ";
					}
				}
				ngram = ngram.substring(0, ngram.length() - 1);
				
				word.set(ngram);
				outresult.set(one,filename);
				context.write(word, outresult);
				//after first ngram generate, use while loop to generate others
				while(itr.hasMoreTokens()) {
					currentToken = itr.nextToken();
					ngram = LastTokens + currentToken;
					LastTokens = LastTokens + currentToken + " ";
					LastTokens = LastTokens.substring(LastTokens.indexOf(" ")+1);
					word.set(ngram);
					outresult.set(one,filename);
					context.write(word, outresult);
				}
			}
		}
	}
	
	/**
	 * Hadoop Combiner method to do the first readuce
	 * if the ngram happened in different files, the output filename will have all of file names.
	 */
	public static class IntSumCombiner extends Reducer<Text, IntTextWritable, Text, IntTextWritable> {
		private IntTextWritable result = new IntTextWritable();;
		private Text word = new Text();

		public void reduce(Text key, Iterable<IntTextWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			// we use a TreeSet to make the file_name not the same and TreeSet have the sort function
			TreeSet<String> filenameset = new TreeSet<String>();
			
			for (IntTextWritable val : values) {
				sum += val.getKey().get();
				filenameset.add(val.getValue().toString());
			}
			// sorting the fileName
			filenameset.comparator();
			//out put: ngram  count  file_name1 file_name2 ...
			String filename = StringUtils.join(filenameset.toArray(), " ");
			word.set(filename);
			result.set(new IntWritable(sum),word);
			context.write(key, result);
		}
	}

	/**
	 * Hadoop map reducer,we need to reducer all the blocks and we have mincount to filter the reasult
	 * almost same as the IntSumCombiner, but we have a filter part in IntSumReducer
	 */
	public static class IntSumReducer extends Reducer<Text, IntTextWritable, Text, IntTextWritable> {
		private IntTextWritable result = new IntTextWritable();;
		private Text word = new Text();
		private int mincount;

		public void reduce(Text key, Iterable<IntTextWritable> values, Context context)
				throws IOException, InterruptedException {
			
			mincount = Integer.valueOf(context.getConfiguration().get("mincount"));
			int sum = 0;
			TreeSet<String> filenameset = new TreeSet<String>();
			
			for (IntTextWritable val : values) {
				sum += val.getKey().get();
				// if the input filename have more, we need to seperate it first than add it in to the set
				StringTokenizer itr = new StringTokenizer(val.getValue().toString());
				while (itr.hasMoreTokens()) {
					filenameset.add(itr.nextToken());
				}
			}
			// to Filter the result 
			if (sum < mincount) {
				return;
			} else {
				filenameset.comparator();
				String filename = StringUtils.join(filenameset.toArray(), " ");
				word.set(filename);
				result.set(new IntWritable(sum),word);
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		//BasicConfigurator.configure();
		// make sure the input is correct
		if (args.length != 4) {
			System.out.println("input not correct ~");
			System.exit(0);
		}
		Configuration conf = new Configuration();
		conf.set("ngram", args[0]);
		conf.set("mincount", args[1]);
		Job job = Job.getInstance(conf, "assignment1");
		job.setJarByClass(Assignment1.class);
		job.setMapperClass(ToNgramMapper.class);
		job.setCombinerClass(IntSumCombiner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntTextWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

/**
 * 
 * individual defined Writable: IntTextWritable<IntWritable,Text>
 * due to the output format need two part, count and file_name,
 * IntTextWritable can make it easy, it implements from Writable class
 *
 */
class IntTextWritable implements Writable {
	//count
	private IntWritable num;
	//filename
	private Text filename;
	
	// Must provide a non-parametric construction method，otherwise it will have null pointer error
	public IntTextWritable() {
		set(new IntWritable(0),new Text());
	}
	// construction function
	public IntTextWritable(IntWritable num,Text filename) {
		set(num,filename);
	}
	// set function for construction function
	public void set(IntWritable num,Text filename) {
		this.num = num;
		this.filename = filename;
	}
	// get the count
	public IntWritable getKey() {
		return num;
	}
	// get the filename
	public Text getValue() {
		return filename;
	}
	// call the object's readFields() method to deserialize each pbject from the input stream
	public void readFields(DataInput arg0) throws IOException {
		num.readFields(arg0);
		filename.readFields(arg0);
	}
	//Serialize each member object into the output stream by the write method of the member object itself
	public void write(DataOutput arg0) throws IOException {
		num.write(arg0);
		filename.write(arg0);			
	}
	// individual defined output format
	public String toString() {
		return num.get() + "  " + filename.toString();
	}
}