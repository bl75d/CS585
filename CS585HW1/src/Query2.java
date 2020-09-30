//***************************
//https://www.edureka.co/blog/mapreduce-example-reduce-side-join/
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query2 {
	public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("CustName "+parts[1]));
		}
	}
	
	public static class TransactionMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[1]), new Text("TransTotal "+parts[2]));
		}
	}
	

	public class JoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String name = "";
			double total = 0.0;
			int count = 0;
			for (Text t : values) {
				String parts[] = t.toString().split("  ");
				if (parts[0].equals("TransTotal")) {
					count++;
					total += Float.parseFloat(parts[1]);
				} else if (parts[0].equals("CustName")) {
					name = parts[1];
				}
			}
			String cust = String.format("%s %s", key.toString(),name);
			String str = String.format("%d %f", count, total);
			context.write(new Text(cust), new Text(str));
		}
	}

 
	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "Reduce-side join");
		 job.setJarByClass(Q2.class);
		 
		 job.setCombinerClass(JoinReducer.class);
		 job.setReducerClass(JoinReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		  
		 MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustomerMapper.class);
		 MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TransactionMapper.class);
		 Path outputPath = new Path(args[2]);
		  
		 FileOutputFormat.setOutputPath(job, outputPath);
		 outputPath.getFileSystem(conf).delete(outputPath);
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 }
}

