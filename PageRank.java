package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.File;
import org.apache.hadoop.fs.FileStatus;



/*Main Class*/
public class PageRank extends Configured implements Tool {

	static final Logger LOG = Logger .getLogger( PageRank.class);
	static ArrayList<String> nodes=new ArrayList<String>();
	static HashMap<String,ArrayList<String>> links=new HashMap<String,ArrayList<String>>(); 
	static HashMap<String,Double> linkvalues=new HashMap<String,Double>(); 
	static HashMap<String,Double> values;
	public static void main( String[] args) throws  Exception {

		String[] allArgs = new String[2];
		allArgs[1]=args[1];
		allArgs[0]=args[0];;
		int res  = ToolRunner .run( new PageRank(), allArgs);
		System .exit(res);
	}


/*To read from the given hadoop directory*/
	public static String readDirectory(String args) throws IOException{
        File file = new File(args);
		FileSystem fileSystem=FileSystem.get(new Configuration());
		FileStatus[] fileStatus = fileSystem.listStatus(new Path(args));
		File[] files = file.listFiles();
		StringBuilder sb=new StringBuilder();  
		for(FileStatus status : fileStatus) {
							
			try {
				Path path=new Path(status.getPath().toString());
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
				String line = br.readLine();
				while (line != null) {
					sb.append(line+'\n');
					line = br.readLine();
				}
				br.close();
			} 
			catch(Exception e) {
				e.printStackTrace();
			}	
		}	
	return sb.toString();
	}

/*writing into a hadoop directory*/
	public static void writeFile(ArrayList<String> nodes, String filename, HashMap<String,ArrayList<String>> links, HashMap<String,Double> values) throws IOException {
		int i=0,j=2;
		try {
			
			FileSystem fs = FileSystem.get(new Configuration());
			Path path=new Path(filename+"/test1");
			Path path2=new Path(filename+"/final");
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
			BufferedWriter br1=new BufferedWriter(new OutputStreamWriter(fs.create(path2,true)));
			String line=null;
			
			for(String adj:nodes){
				ArrayList<String> link=new ArrayList<String>();
				link=links.get(adj.trim());
				//Separate titles from the value using "==!=="
				line=adj+"==!==";
				if(link!=null){
					for(String string:link){
						//Separate each value with "#####"
						line=line+string+"#####";
					}
					line=line+"==!=="+values.get(adj);
					i=i+1;
	 
					if(i>1000){
						br.close();
						path=new Path(filename+"/test"+j);
						j=j+1;
						br=new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
						i=0;
					}
					br.write(line);
					br1.write(line);
					br.newLine();
					br1.newLine();
				}
			}
	  
			br.close(); 
			br1.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}  

//Read a file and return the contents
	public static String readFile(String fileName) throws IOException {
		try {
			Path path=new Path(fileName);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			//StringBuilder sb = new StringBuilder(Integer.MAX_VALUE);
			StringBuffer sb=new StringBuffer();
			while (line != null) {
				sb.append(line);
				sb.append('\n');
				line = br.readLine();
			}
			String temp=sb.toString();
			sb.delete(0, sb.length());
			return temp;
		} 
		catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}	 
 

	//To return a HashMap of PageRank values
	public static HashMap<String,Double> computePageRankValues(ArrayList<String> nodes, HashMap<String,ArrayList<String>> links, HashMap<String,Double> inLinks){
		double n=nodes.size();
		HashMap<String,Double> pageRankValues=new HashMap<String,Double>();
		for(String node:nodes){
			pageRankValues.put(node,(1/n));
			//Each is set as 1/N since it is the initial value
		} 
		return pageRankValues;

	}


	//The Run method
	public int run( String[] args) throws  Exception {
		//First MR job that parses the xml file
		Job job  = Job .getInstance(getConf(), "parse");
		Job jobLinkGraph  = Job .getInstance(getConf(), "links");
		Configuration configuration=new Configuration();
		//This is to set the stack size
		configuration.set("mapred.child.java.opts","-Xmx4028m -Xss600m");
		//configuration.set("mapreduce.task.io.sort.mb","-Xmx2000m");
		Job jobOutLink  = new Job(configuration);
		Job jobInvIndex= new Job(getConf(),"InvertedIndex");
		Job jobRanks  = Job .getInstance(getConf(), "Rank");
		
		//Setting one reduce task
		job.setNumReduceTasks(1);
		int i=1;
		job.setJarByClass( this .getClass());
		Path tempPath=new Path(args[1]);
		FileInputFormat.addInputPaths(job,  args[0]);
		FileOutputFormat.setOutputPath(job,  tempPath);
		job.setMapperClass( Map .class);
		job.setReducerClass( Reduce .class);
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass( Text .class);
		if(job.waitForCompletion(true)){
			//Second MR job that finds the # of inlinks in the doc
			Configuration configuration1=new Configuration();
			configuration1.set("mapred.child.java.opts","-Xmx4028m -Xss600m");
			//configuration1.set("mapreduce.task.io.sort.mb","400m");
			jobLinkGraph  = new Job(configuration1);
			jobLinkGraph.setNumReduceTasks(1);
			jobLinkGraph.setJarByClass( this .getClass());
			FileInputFormat.addInputPaths(jobLinkGraph,  args[1]);
			FileOutputFormat.setOutputPath(jobLinkGraph,  new Path(args[1]+"/output/"));
				  
			jobLinkGraph.setMapperClass( MapInLink .class);
			jobLinkGraph.setReducerClass( ReduceInLink .class);
			jobLinkGraph.setOutputKeyClass( Text .class);
			jobLinkGraph.setOutputValueClass( Text .class);
			
			

			if(jobLinkGraph.waitForCompletion(true)){

				//Third MR job that finds the outlinks from the in links graph 
				jobOutLink.setNumReduceTasks(1);

				jobOutLink.setJarByClass( this .getClass());
				FileInputFormat.addInputPaths(jobOutLink,  args[1]+"/output/");
				FileOutputFormat.setOutputPath(jobOutLink,  new Path(args[1]+"/output/link/"));

				Pattern WORD_BOUNDARY = Pattern .compile("\n");
				jobOutLink.setMapperClass( MapOutLink .class);
				jobOutLink.setReducerClass( ReduceOutLink .class);
				jobOutLink.setOutputKeyClass( Text .class);
				jobOutLink.setOutputValueClass( Text .class);

				if(jobOutLink.waitForCompletion(true)){
					
					FileSystem fs=FileSystem.get(new Configuration());
					Path path=new Path(args[1]+"part-r-00000");
					/*if(fs.exists(path))
					fs.delete(path,true);*/
					
					String totalLinks=readFile(args[1]+"/output/link/part-r-00000");
					for ( String word  : WORD_BOUNDARY .split(totalLinks)) {
						if (word.isEmpty()) {
						   continue;
						}

						String[] value=word.split("==!==");

						linkvalues.put(value[0].trim(),Double.parseDouble(value[1].trim()));
					}
					String inlinks=readFile(args[1]+"/output/part-r-00000");
						  
					for ( String word  : WORD_BOUNDARY .split(inlinks)) {
						if (word.isEmpty()) {
						   continue;
						}
						String[] firstNode=word.split("==!==");
						String[] remainingNode=firstNode[1].split(Pattern.quote("#####"));
						ArrayList<String> al=new ArrayList<>();
						if(!(firstNode[0].equals(null))){
							
							if(!(nodes.contains(firstNode[0].trim()))){
								nodes.add(firstNode[0].trim());
							}
							for(i=0;i<remainingNode.length;i++){
								al.add(remainingNode[i].trim());
								if(!(nodes.contains(remainingNode[i].trim())))
								{
									nodes.add(remainingNode[i].trim());
								}
								links.put(firstNode[0].trim(),al);

							}
						}
					}
					values=computePageRankValues(nodes,links,linkvalues);
					
					// File with the initial page rank value
					writeFile(nodes,args[1]+"/output/link/rank/rank0",links,values);
						  
					WORD_BOUNDARY=Pattern .compile("\r\n");
					// Run 10 Iterations of MR jobs
					for(i=1;i<=10;i++){
						String read="";
						if (i==1){
							read=readFile(args[1]+"/output/link/rank/rank"+(i-1)+"/final");
						}
						if(i>1){
							read=readDirectory(args[1]+"/output/link/rank/rank"+(i-1));
						}
						for ( String word  : WORD_BOUNDARY .split(read)) {
							if (word.isEmpty()) {
								continue;
							}
							String[] temp=word.split("==!==");
							if(temp.length==3){
								values.put(temp[0].trim(),Double.parseDouble(temp[2].trim()));
							}
						}

						Configuration configuration2=new Configuration();
						configuration2.set("test", values.keySet()+"==!=="+values.values());
						configuration2.set("test1", linkvalues.keySet()+"==!=="+linkvalues.values());
						configuration2.set("mapred.child.java.opts","-Xmx4028m -Xss600m");
						//configuration2.set("mapreduce.task.io.sort.mb","400m");
						jobRanks  = new Job(configuration2);
						jobRanks.setJarByClass( this .getClass());

						if(i==1)
						{
							File file=new File(args[1]+"/output/link.rank"+(i-1)+"/final");
							file.delete();      
						}
						//Clear every previous file that are not required anymore
						if(i>=2){
							fs=FileSystem.get(new Configuration());
							path=new Path(args[1]+"/output/link/rank/rank"+(i-2));
							if(fs.exists(path))
							fs.delete(path,true); 
						}
						FileInputFormat.addInputPaths(jobRanks,  args[1]+"/output/link/rank/rank"+(i-1)+"/");
						FileOutputFormat.setOutputPath(jobRanks,  new Path(args[1]+"/output/link/rank/rank"+(i)+"/"));
						jobRanks.setMapperClass( MapRank .class);
						jobRanks.setReducerClass( ReduceRank .class);
						jobRanks.setOutputKeyClass( Text .class);
						jobRanks.setOutputValueClass( Text .class);
						MultipleOutputs.addNamedOutput(jobRanks, "final", TextOutputFormat.class, LongWritable.class, Text.class);
						jobRanks.waitForCompletion(true);
					}
					fs=FileSystem.get(new Configuration());
					path=new Path(args[1]+"output/part-r-00000");
					if(fs.exists(path))
					fs.delete(path,true);
					fs=FileSystem.get(new Configuration());
					path=new Path(args[1]+"output/link/part-r-00000");
					if(fs.exists(path))
					fs.delete(path,true);
				}
			}
		} 
     
		Configuration configuration3=new Configuration();
		configuration3.set("mapred.child.java.opts","-Xmx4028m -Xss600m");
		//configuration3.set("mapreduce.task.io.sort.mb","400m");
		
		//Job that sorts the page values in descending order
		Job jobSort  = new Job(configuration3);
		jobSort.setJarByClass( this .getClass());
		//As mentioned in the question, setting the reducer tasks to 1
		jobSort.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(jobSort ,  args[1]+"/output/link/rank/rank"+(i-1)+"/");
		FileOutputFormat.setOutputPath(jobSort ,  new Path(args[1]+"/output/link/rank/sort/"));
		jobSort.setMapperClass( MapSort .class);
		jobSort.setReducerClass( ReduceSort .class);
		jobSort.setOutputKeyClass( DoubleWritable .class);
		jobSort.setOutputValueClass( Text .class);
		
		//Clear the files upon sorting
		FileSystem fs=FileSystem.get(new Configuration());
		Path path=new Path(args[1]+"/output/link/rank/rank"+(i-2));
		if(fs.exists(path))
		fs.delete(path,true);
		if(jobSort.waitForCompletion(true)){
			path=new Path(args[1]+"/output/link/rank/rank"+(i-1));
			if(fs.exists(path))
			fs.delete(path,true);
			//Additional MR job that calculates the Inverted index
			Configuration configuration4=new Configuration();
			configuration4.set("mapred.child.java.opts","-Xmx4028m -Xss600m");
			jobInvIndex= new Job(configuration4);
			jobInvIndex.setNumReduceTasks(1);
			jobInvIndex.setJarByClass( this .getClass());
			FileInputFormat.addInputPaths(jobInvIndex,  args[1]);
			FileOutputFormat.setOutputPath(jobInvIndex,  new Path(args[1]+"/output/link/rank/invertedindex/"));
				  
			jobInvIndex.setMapperClass( MapInvertedIndex .class);
			jobInvIndex.setReducerClass( ReduceInvertedIndex .class);
			jobInvIndex.setOutputKeyClass( Text .class);
			jobInvIndex.setOutputValueClass( Text .class);
			
			if(jobInvIndex.waitForCompletion(true)){
				path=new Path(args[1]+"part-r-00000");
				if(fs.exists(path))
				fs.delete(path,true);
			}
		}
		return jobSort.waitForCompletion( true)  ? 0 : 1;
	}	

   
   /*Parse the files to get links and their outlinks*/
    public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		private static final Pattern WORD_BOUNDARY = Pattern .compile("\r\n");
		public void map( LongWritable offset,  Text lineText,  Context context) throws  IOException,  InterruptedException {
			try{
				String wordFile="";
				String line  = lineText.toString();
				Text currentWord  = new Text();

				for ( String word  : WORD_BOUNDARY .split(line)) {
					if (word.isEmpty()) {
					   continue;
					}
					wordFile="<?xml version=\"1.0\"?> <class>"+word+"</class>";
					DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
					DocumentBuilder builder = factory.newDocumentBuilder();
					ByteArrayInputStream input =  new ByteArrayInputStream(wordFile.getBytes("UTF-8"));
					Document doc = builder.parse(input);
					doc.getDocumentElement().normalize();
					//Parser that extracts title and text
					String title = doc.getElementsByTagName("title").item(0).getTextContent();
					String links = doc.getElementsByTagName("text").item(0).getTextContent();
					currentWord  = new Text(title);
					Pattern p = Pattern.compile("\\[\\[.*?]\\]");
					Matcher m = p.matcher(links);
					String current =null;
					while(m.find()) {
						current =m.group().replace("[[", "").replace("]]", "");
						if(!(title.trim().equals(null))&&!(title.trim().equals(""))){				
							context.write(currentWord,new Text(current));
						}
					}		
				}
			}
			catch(SAXException e){
			}
			catch(ParserConfigurationException e){
			}
		}
	}

	public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)throws IOException,  InterruptedException {
			String all="";
			Text last=new Text(word.toString()+"==!==");
			for ( Text count  : counts) {
				all  =all+ count.toString()+"#####";
			}
			context.write(last,  new Text(all.substring(0,all.length()-5).trim()));
		}
	}


/*this MR gets links and outputs the in-links*/
	public static class MapInLink extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		private static final Pattern WORD_BOUNDARY = Pattern .compile("\r\n");

		public void map( LongWritable offset,  Text lineText,  Context context)throws  IOException,  InterruptedException {
			String line  = lineText.toString();
			for ( String word  : WORD_BOUNDARY .split(line)) {
				if (word.isEmpty()) {
				   continue;
				}
				String[] wordFile=word.split("==!==");
				String links=wordFile[1].trim();
				String[] inlinks=links.split(Pattern.quote("#####"));
				for(int i=0;i<inlinks.length;i++) {
					context.write(new Text(inlinks[i].trim()),new Text(wordFile[0].trim()));
				}
			}
		}
	}

	public static class ReduceInLink extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context) throws IOException,  InterruptedException {
			String all="";
			for ( Text count  : counts) {
				all=all+count+"#####";
			}
			context.write(new Text(word.toString()+"==!=="),  new Text(all));
		}
	}
	
	//This class calculates the inverted index (Additional Task)
	public static class MapInvertedIndex extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		private static final Pattern WORD_BOUNDARY = Pattern .compile("\r\n");

		public void map( LongWritable offset,  Text lineText,  Context context)throws  IOException,  InterruptedException {
			String line  = lineText.toString();
			ArrayList<String> words=new ArrayList<>();
			ArrayList<String> docs=new ArrayList<>();
			for ( String word  : WORD_BOUNDARY .split(line)) {
				if (word.isEmpty()) {
				   continue;
				}
				String[] wordFile=word.split("==!==");
				String links=wordFile[1].trim();
				String[] inlinks=links.split(Pattern.quote("#####"));
				for(int i=0;i<inlinks.length;i++) {
					//if the word is not added, add it and its doc into another array list
					if(!words.contains(inlinks[i].trim())){
						words.add(inlinks[i].trim());
						docs.add(wordFile[0].trim());
					}
					//if the word is already added, append its doc name to the existing list 

					else if(words.contains(inlinks[i].trim())){
						for(int j=0;j<words.size();j++){
							if(words.get(j).equals(inlinks[i])){
								docs.set(j, docs.get(j)+"+++++"+wordFile[0].trim());
							}
						}
					}
				}
			}
			for(int i=0;i<words.size();i++)
				context.write(new Text(words.get(i).trim()),new Text(docs.get(i).trim()));
		}
	}

	public static class ReduceInvertedIndex extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context) throws IOException,  InterruptedException {
			
			for ( Text count  : counts) {
				context.write(new Text(word.toString()+"==!=="),  new Text(count.toString()));
			}
		}
	}

//this MR gets nodes and outputs their outlinks count

	public static class MapOutLink extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		private static final Pattern WORD_BOUNDARY = Pattern .compile("\r\n");
		
		public void map( LongWritable offset,  Text lineText,  Context context) throws  IOException,  InterruptedException {
			String line  = lineText.toString();
			for ( String word  : WORD_BOUNDARY .split(line)) {
				if (word.isEmpty()) {
					continue;
				}

				String[] wordFile=word.split("==!==");
				String links=wordFile[1];
				String[] inlinks=links.split(Pattern.quote("#####"));
				for(int i=0;i<inlinks.length;i++){
					context.write(new Text(inlinks[i].trim()),new Text("1"));
				}
			}
		}
	}

	public static class ReduceOutLink extends Reducer<Text ,  Text ,  Text ,  IntWritable > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context) throws IOException,  InterruptedException {
			int sum=0;
			for ( Text count  : counts) {
				 sum =sum+1;
			}
			context.write(new Text(word.toString()+"==!=="),  new IntWritable(sum));
		}
	}



/*this MR calculates rank values through 10 iterations*/
	public static class MapRank extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		
		private static final Pattern WORD_BOUNDARY = Pattern .compile("\n");
		public void map( LongWritable offset,  Text lineText,  Context context) throws  IOException,  InterruptedException {
			Configuration configuration = context.getConfiguration();
			String line  = lineText.toString();
			String dumps=configuration.get("test");
			String[ ]dummyFiles=dumps.split("==!==");
			String[] first=dummyFiles[0].substring(1,dummyFiles[0].length()-1).split(",");
			String[] second=dummyFiles[1].substring(1,dummyFiles[1].length()-1).split(",");
			HashMap<String,Double>  mapValues = new HashMap<String,Double>();
			
			for(int i=0;i<second.length;i++){
				mapValues.put(first[i].trim(),Double.parseDouble(second[i].trim()));
			}
			
			for ( String word  : WORD_BOUNDARY .split(line)) {
				if (word.isEmpty()) {
				   continue;
				}
  
				String[] newValues=word.split("==!=="); 
				String[] newRanks=newValues[1].split(Pattern.quote("#####"));
				for(int i=0;i<newRanks.length;i++){
					context.write(new Text(newValues[0].trim()),new Text(newRanks[i].trim()+"==!=="+mapValues.get(newRanks[i].trim()))); 
				}
			}
        }
	}
 
	public static class ReduceRank extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {   
		
		public void reduce( Text word,  Iterable<Text > counts,  Context context) throws IOException,  InterruptedException {
			String test="";

			Configuration configuration = context.getConfiguration();

			String dumps=configuration.get("test1");
			String[ ]dummyValues=dumps.split("==!==");
			String[] first=dummyValues[0].substring(1,dummyValues[0].length()-1).split(",");
			String[] second=dummyValues[1].substring(1,dummyValues[1].length()-1).split(",");

			HashMap<String,Double> mapValues = new HashMap<String,Double>();

			for(int i=0;i<second.length;i++){
				mapValues.put(first[i].trim(),Double.parseDouble(second[i].trim()));
			}
			double rank=0.0;
			for(Text count : counts){
				String[] temp=count.toString().split("==!==");
				test=test+temp[0]+"#####";
				String[] newValues=count.toString().split("==!=="); 
				if(!(newValues[1].trim().equals("null"))&&!(newValues[1].trim().equals(null))){
					if(mapValues.containsKey(newValues[0].trim())){
						rank=rank+((Double.parseDouble(newValues[1].trim()))/mapValues.get(newValues[0].trim()));
						LOG.info(Double.toString((Double.parseDouble(newValues[1].trim())/mapValues.get(newValues[0].trim()))));
					}
				}
				
							   
			}
			//Calculate the Page rank value
			rank=(1-0.85)+(0.85*(rank));
			context.write(new Text(word.toString().trim()+"==!=="+test.trim()+"==!=="),  new DoubleWritable(rank));
		}
    }
	
	//Sort the Page Rank values in descending order
	public static class MapSort extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {

		private static final Pattern WORD_BOUNDARY = Pattern .compile("\n");
		public void map( LongWritable offset,  Text lineText,  Context context)throws  IOException,  InterruptedException {

			String line  = lineText.toString();

			for ( String word  : WORD_BOUNDARY .split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				String[] newValues=word.split("==!=="); 
				Double val=Double.valueOf(newValues[2].trim());
				
				//To sort in descending order use -1. If ascending order leave it as such
				context.write(new DoubleWritable(val*-1),new Text(newValues[0].trim())); 
			}
		}
	}

    public static class ReduceSort extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( DoubleWritable word,  Iterable<Text > counts,  Context context) throws IOException,  InterruptedException {
			for(Text count : counts){
				context.write(new Text(count+""),  new DoubleWritable((word.get()*-1)));         
			}
			
		} 
    }
    
}
