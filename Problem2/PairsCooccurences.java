import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairsCooccurences {

	public static class PairsCoocMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException,InterruptedException { 
			String text = line.toString(); 
			String[] terms = text.split("\\s+"); 
			for (int i = 0; i < terms.length; i++) {	 
				// skip empty tokens 
				if (terms[i].length() == 0) 
					continue;  
				for (int j = 0; j < terms.length; j++){ 
					if (j == i) 
						continue;  
					// skip empty tokens 
					if (terms[j].length() == 0) 
						continue; 
					Text pairs = new Text(terms[i]+", "+terms[j]);
					context.write(pairs, one); 
				} 
			} 
		}
	}	
	
	private static class PairsCoocReducer extends Reducer<Text, IntWritable, Text, IntWritable>{ 
		private IntWritable SumValue = new IntWritable(); 
 
		@Override 
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
			int sum = 0; 
			for (IntWritable value : values){ 
				sum += value.get(); 
			} 

			SumValue.set(sum); 
			context.write(key, SumValue); 
		} 
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Co-occurences using pairs");
		job.setJarByClass(PairsCooccurences.class);
		job.setMapperClass(PairsCoocMapper.class);
		job.setCombinerClass(PairsCoocReducer.class);
		job.setReducerClass(PairsCoocReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
