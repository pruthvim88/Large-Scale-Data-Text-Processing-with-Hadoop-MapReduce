import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LatinWordCount {
	
	static HashMap<String,ArrayList<String>> lemmaMapList = new HashMap<String,ArrayList<String>>();

	public static class LatinWCMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException,InterruptedException { 
			try{
				String inputSplit = ">";
				String[] input = line.toString().split(inputSplit);
				Text location = new Text(input[0]+inputSplit);
				input[1] = input[1].replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+|\\t+", " ");	
				String[] words = input[1].split("\\s+");
				for (String word: words){
					word = word.replaceAll("j", "i");
					word = word.replaceAll("v", "u");
					if (lemmaMapList.containsKey(word.toLowerCase())){
						ArrayList<String> lemmas = lemmaMapList.get(word);
						for (String lemma:lemmas) {
							Text modlemma = new Text(lemma);
							context.write(modlemma, location);			
						}			
					}
					else{
						if(!word.isEmpty())
							context.write(new Text(word), location); 
					}
						
				}
			}
			catch (Exception e) { 
				//e.printstacktrace();
			}	
		}
	}	
	
	public static class LatinWCReducer extends Reducer<Text,Text,Text,Text>{ 

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String text = "[";
			int counter = 0;
			for (Text val:values) {
				counter++;
				text += val.toString() + ",";
			}
			text+="Count: "+counter+"]";
			Text output = new Text(text);
			context.write(key, output);
		}
	}

	public static void main(String[] args) throws Exception {
		String csvFile = "/media/sf_Lab4/Problem3/new_lemmatizer.csv";		
		String cvsSplitBy = ",";
		String line = "";
		BufferedReader br = new BufferedReader(new FileReader(csvFile));
		while ((line = br.readLine()) != null) {
		String[] lemmas = line.split(cvsSplitBy);
			if(lemmas[0]!=null){
				ArrayList<String> lemmaList = new ArrayList<String>();
				for(int i=1;i<lemmas.length;i++){
					lemmaList.add(lemmas[i]);
				}
				if(lemmaList.size()>0)
					lemmaMapList.put(lemmas[0],lemmaList);
			}	
		}     
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Wordcount on Classical Latin text");
		job.setJarByClass(LatinWordCount.class);
		job.setMapperClass(LatinWCMapper.class);
		//job.setCombinerClass(LatinWCReducer.class);
		job.setReducerClass(LatinWCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
