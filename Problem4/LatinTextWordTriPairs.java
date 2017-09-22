import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Locale;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LatinTextWordTriPairs {
	
	static HashMap<String,ArrayList<String>> lemmaMapList = new HashMap<String,ArrayList<String>>();

	public static class LatinWTriPairsMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException,InterruptedException { 
			try{
				String inputSplit = ">";
				String[] input = line.toString().split(inputSplit);
				Text location = new Text(input[0]+inputSplit);
				input[1] = input[1].replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+|\\t+", " ");
				String[] words = input[1].split("\\s+");
				for (int i=0;i<words.length-2;i++){
					words[i] = words[i].replaceAll("j", "i");
					words[i] = words[i].replaceAll("v", "u");
					words[i+1] = words[i+1].replaceAll("j", "i");
					words[i+1] = words[i+1].replaceAll("v", "u");
					words[i+2] = words[i+2].replaceAll("j", "i");
					words[i+2] = words[i+2].replaceAll("v", "u");
					ArrayList<String> w1_lemmas = new ArrayList<String>();
					ArrayList<String> w2_lemmas = new ArrayList<String>();
					ArrayList<String> w3_lemmas = new ArrayList<String>();
					if(lemmaMapList.containsKey(words[i].toLowerCase()) && lemmaMapList.containsKey(words[i+1].toLowerCase()) && lemmaMapList.containsKey(words[i+2].toLowerCase())){				
						w1_lemmas = lemmaMapList.get(words[i]);
						w2_lemmas = lemmaMapList.get(words[i+1]);
						w3_lemmas = lemmaMapList.get(words[i+2]);
						for(String w1_lemma:w1_lemmas){
							for(String w2_lemma:w2_lemmas){
								for(String w3_lemma:w3_lemmas){
									context.write(new Text(w1_lemma + ","+w2_lemma+","+w3_lemma), location);
								}								
							}
						}	
					}
					else if(lemmaMapList.containsKey(words[i].toLowerCase()) && lemmaMapList.containsKey(words[i+1].toLowerCase()) && !lemmaMapList.containsKey(words[i+2].toLowerCase())){
						if(words[i+2].isEmpty())
							continue;
						w1_lemmas = lemmaMapList.get(words[i]);
						w2_lemmas = lemmaMapList.get(words[i+1]);
						for(String w1_lemma:w1_lemmas){
							for(String w2_lemma:w2_lemmas){
								context.write(new Text(w1_lemma+","+w2_lemma+","+words[i+2] ), location);
							}	
						}
					}
					else if(lemmaMapList.containsKey(words[i].toLowerCase()) && !lemmaMapList.containsKey(words[i+1].toLowerCase()) && lemmaMapList.containsKey(words[i+2].toLowerCase())){
						if(words[i+1].isEmpty())
							continue;
						w1_lemmas = lemmaMapList.get(words[i]);
						w3_lemmas = lemmaMapList.get(words[i+2]);
						for(String w1_lemma:w1_lemmas){
							for(String w3_lemma:w3_lemmas){
								context.write(new Text(w1_lemma+","+words[i+1]+","+w3_lemma), location);
							}	
						}
					}
					else if(!lemmaMapList.containsKey(words[i].toLowerCase()) && lemmaMapList.containsKey(words[i+1].toLowerCase()) && lemmaMapList.containsKey(words[i+2].toLowerCase())){
						if(words[i].isEmpty())
							continue;
						w2_lemmas = lemmaMapList.get(words[i+1]);
						w3_lemmas = lemmaMapList.get(words[i+2]);
						for(String w2_lemma:w2_lemmas){
							for(String w3_lemma:w3_lemmas){
								context.write(new Text(words[i]+","+w2_lemma+","+w3_lemma), location);
							}	
						}
					}
					else if(lemmaMapList.containsKey(words[i].toLowerCase()) && !lemmaMapList.containsKey(words[i+1].toLowerCase()) && !lemmaMapList.containsKey(words[i+2].toLowerCase())){
						if(words[i+1].isEmpty() || words[i+2].isEmpty())
							continue;
						w1_lemmas = lemmaMapList.get(words[i]);
						for(String w1_lemma:w1_lemmas){
							context.write(new Text(w1_lemma+","+words[i+1]+","+words[i+2]), location);
						}
					}
					else if(!lemmaMapList.containsKey(words[i].toLowerCase()) && lemmaMapList.containsKey(words[i+1].toLowerCase()) && !lemmaMapList.containsKey(words[i+2].toLowerCase())){
						if(words[i].isEmpty() || words[i+2].isEmpty())
							continue;
						w2_lemmas = lemmaMapList.get(words[i+1]);
						for(String w2_lemma:w2_lemmas){
							context.write(new Text(words[i]+","+w2_lemma+","+words[i+2]), location);
						}
					}
					else if(!lemmaMapList.containsKey(words[i].toLowerCase()) && !lemmaMapList.containsKey(words[i+1].toLowerCase()) && lemmaMapList.containsKey(words[i+2].toLowerCase())){
						if(words[i].isEmpty() || words[i+1].isEmpty())
							continue;
						w3_lemmas = lemmaMapList.get(words[i+2]);
						for(String w3_lemma:w3_lemmas){
							context.write(new Text(words[i]+","+words[i+1]+","+w3_lemma), location);
						}
					}
					else{
						if(words[i].isEmpty() || words[i+1].isEmpty() || words[i+2].isEmpty())
							continue;
						context.write(new Text(words[i]+ ","+words[i+1]+ ","+words[i+2]), location);
					}
				}
			}
			catch (Exception e) { 
				//e.printstacktrace();
			}	
		}
	}	
	
	public static class LatinWTriPairsReducer extends Reducer<Text,Text,Text,Text>{ 

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
		String csvFile = "/media/sf_Lab4/Problem4/new_lemmatizer.csv";		
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
		Job job = Job.getInstance(conf, "3 Word co-occurence for  on Classical Latin text using pairs");
		job.setJarByClass(LatinTextWordTriPairs.class);
		job.setMapperClass(LatinWTriPairsMapper.class);
		//job.setCombinerClass(LatinWCReducer.class);
		job.setReducerClass(LatinWTriPairsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US).format(new Timestamp(System.currentTimeMillis()));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + timeStamp));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
