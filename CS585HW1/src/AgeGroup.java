import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AgeGroup {

	public static HashMap<String, String> ageMap = new HashMap<String, String>();
	public static HashMap<String, Integer> cntMap = new HashMap<String, Integer>();

  public static class AgeGroupMapper 
       extends Mapper<LongWritable, Text, Text, Text>{
	  
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

    	String[] line = value.toString().split(",");     
        
    	String ageGroup = ageMap.get(line[1].trim());
    	String transTotal = line[2].trim();
    	context.write(new Text(ageGroup), new Text(transTotal));
    }
  }
  
  public static class AgeGroupReducer 
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, 
    		Context context
                       ) throws IOException, InterruptedException {

    	float max = Float.MIN_VALUE;
    	float min = Float.MAX_VALUE;
    	float sum = 0;
    	int number = 0;
    	
    	for (Text value: values) {
    		String[] val = value.toString().split(",");
    		float trans = Float.parseFloat(val[0]);
    		if(trans > max) {
				max = trans;
			}
			if(trans < min) {
				min = trans;
			}
			sum += trans;
			number += 1;
    	}
    	if(number == 0)	number = 1;
    	
		context.write(key, new Text(Float.toString(min) + ", " + Float.toString(max) + ", " + Float.toString(sum / number)));

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
    	String age = items[2];
    	String gender = items[3];
    	int val = Integer.valueOf(age);
    	String group = "";
    	if(val >= 60) {
    		group = "[60, 70]";
    	} else if (val >= 50){
    		group = "[50, 60)";
    	} else if (val >= 40){
    		group = "[40, 50)";
    	} else if (val >= 30){
    		group = "[30, 40)";
    	} else if (val >= 20){
    		group = "[20, 30)";
    	} else if (val >= 10){
    		group = "[10, 20)";
    	}
    	group += ", " + gender;
    	ageMap.put(items[0], group);
    	if (!cntMap.containsKey(group)) {
    		cntMap.put(group, 0);
    	}
    	cntMap.put(group, cntMap.get(group)+1);
//    	System.out.println(data);
    	data = inStream.readLine();
    }
    inStream.close();
    
    
    
    Job job =new Job(conf, "Age Group");
    
    job.setJarByClass(AgeGroup.class);
    job.setMapperClass(AgeGroupMapper.class);
//    job.setMapOutputKeyClass(Text.class);
//    job.setMapOutputValueClass(Text.class);
//    job.setCombinerClass(MinMaxReducer.class);
    job.setReducerClass(AgeGroupReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
//    MultipleInputs.addInputPath(job, new Path("/Users/merqurius/Workspace/CS585Data/Customer.txt"));
//    MultipleInputs.addInputPath(job, new Path("/Users/merqurius/Workspace/CS585Data/Transaction.txt"));
    
    System.exit(job.waitForCompletion(true) ?0 : 1);
  }
}
