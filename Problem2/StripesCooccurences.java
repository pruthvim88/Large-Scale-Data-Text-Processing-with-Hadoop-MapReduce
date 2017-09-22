import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Writable;

public class StripesCooccurences {
	
	public static class MyMapWritable extends MapWritable {
		@Override
		public String toString(){
			StringBuilder result = new StringBuilder();
			Set<Writable> keySet = this.keySet();

			for (Object key : keySet) {
				result.append("{" + key.toString() + " = " + this.get(key) + "}");
			}
			return result.toString();
		}
	}

	public static class StripesCoocMapper extends Mapper<LongWritable, Text, Text, MyMapWritable> {
		private MyMapWritable coocMap = new MyMapWritable();
		private Text word = new Text();
		private static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException,InterruptedException { 
			String text = line.toString(); 
			String[] terms = text.split("\\s+"); 
			coocMap.clear();
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
					Text adjWord = new Text(terms[j]);
					if (coocMap.containsKey(adjWord)) { 
						IntWritable tmp = (IntWritable) coocMap.get(adjWord);
						coocMap.put(adjWord, new IntWritable(tmp.get()+1)); 
					} 
					else{ 
						coocMap.put(adjWord, one); 
					} 					
					word.set(terms[i]);
					context.write(word, coocMap);
				} 
			} 
		}
	}	
	
	private static class StripesCoocReducer extends Reducer<Text, MyMapWritable, Text, MyMapWritable>{ 
		private static MyMapWritable output = new MyMapWritable(); 
		@Override 
		public void reduce(Text key, Iterable<MyMapWritable> values, Context context) throws IOException, InterruptedException { 
			output.clear();
			for (MyMapWritable coocMap : values){
				Set<Writable> keys = coocMap.keySet();
				for (Writable keytext : keys){
					IntWritable fromCount = (IntWritable) coocMap.get(keytext);
					if (output.containsKey(keytext)) {
						IntWritable count = (IntWritable) output.get(keytext);
						count.set(count.get() + fromCount.get());
						output.put(keytext, count);
					} 
					else{
						output.put(keytext, fromCount);
					}
				}
			}
			context.write(key, output);
		} 
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Co-occurences using Stripes");
		job.setJarByClass(StripesCooccurences.class);
		job.setMapperClass(StripesCoocMapper.class);
		job.setCombinerClass(StripesCoocReducer.class);
		job.setReducerClass(StripesCoocReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyMapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
