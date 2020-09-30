import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MinMax {

	public static HashMap<String, String> cCodeMap = new HashMap<String, String>();
	public static HashMap<String, Integer> custMap = new HashMap<String, Integer>();

  public static class MinMaxMapper 
       extends Mapper<LongWritable, Text, Text, Text>{
	  
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

    	String[] line = value.toString().split(",");     
        
    	String countryCode = cCodeMap.get(line[1].trim());
    	String ID = line[1].trim();
    	String transTotal = line[2].trim();
    	context.write(new Text(countryCode), new Text( ID + ", " + transTotal ));
    }
  }
  
  public static class MinMaxReducer 
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, 
    		Context context
                       ) throws IOException, InterruptedException {

    	Vector<Integer> users = new Vector<Integer>();
    	
    	float max = Float.MIN_VALUE;
    	float min = Float.MAX_VALUE;
    	for (Text value: values) {
    		String[] val = value.toString().split(",");
    		int userid = Integer.parseInt(val[0]);
    		if (!users.contains(userid)) {
    			users.add(userid);
    		}
    		float trans = Float.parseFloat(val[1]);
    		if(trans > max) {
				max = trans;
			}
			if(trans < min) {
				min = trans;
			}
    	}
    	
    	int size = users.size();
    	
		context.write(key, new Text(Integer.toString(custMap.get(key.toString())) + ", " + Float.toString(min) + ", " + Float.toString(max)));

    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf =new Configuration();
    
    String[] otherArgs =new GenericOptionsParser(conf, args).getRemainingArgs();
    
    FileSystem fs = FileSystem.get(conf);
    Path file = new Path(otherArgs[0]);
    System.out.println(file);
    FSDataInputStream inStream = fs.open(file);
    String data = inStream.readLine();
    while(data != null) {
    	String[] items = data.split(",");
    	String countrycode = items[4];
    	cCodeMap.put(items[0], countrycode);
    	if (!custMap.containsKey(countrycode)) {
    		custMap.put(countrycode, 0);
    	}
    	custMap.put(countrycode, custMap.get(countrycode)+1);
//    	System.out.println(data);
    	data = inStream.readLine();
    }
    inStream.close();
    
    
    
    Job job =new Job(conf, "Min Max");
    
    job.setJarByClass(MinMax.class);
    job.setMapperClass(MinMaxMapper.class);
//    job.setMapOutputKeyClass(Text.class);
//    job.setMapOutputValueClass(Text.class);
//    job.setCombinerClass(MinMaxReducer.class);
    job.setReducerClass(MinMaxReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    
    System.exit(job.waitForCompletion(true) ?0 : 1);
  }
}
