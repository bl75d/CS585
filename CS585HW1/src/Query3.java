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

public class Query3 {
	public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			String namesalary=parts[1]+","+parts[5];
			context.write(new Text(parts[0]), new Text("CustNameandsalary "+namesalary));
		}
	}
	
	public static class TransactionMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			String transinfo=parts[2]+","+parts[3];
			context.write(new Text(parts[1]), new Text("TransInfo "+transinfo));
		}
	}
	
	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String cust = "";
			double total = 0.0;
			int count = 0;
			int items=0;
			int minitems=10;
			for (Text t : values) {
				String parts[] = t.toString().split(" ");
				if (parts[0].equals("TransInfo")) {
					count++;
					total += Float.parseFloat(parts[1].split(",")[0]);
					items=Integer.parseInt(parts[1].split(",")[1]);
					if(items<minitems) {
						minitems=items;
					}
				} else if (parts[0].equals("CustNameandsalary")) {
					cust = key.toString()+","+parts[1];
				}
			}
			String str = String.format("%d %f %d", count, total, minitems);
			context.write(new Text(cust), new Text(str));
		}
	}

 
	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "Reduce-side join");
		 job.setJarByClass(Query3.class);
		 
		 job.setReducerClass(JoinReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		  
//		 MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustomerMapper.class);
//		 MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TransactionMapper.class);
		 MultipleInputs.addInputPath(job, new Path("/Users/merqurius/Workspace/CS585Data/Customer.txt"),TextInputFormat.class, CustomerMapper.class);
		 MultipleInputs.addInputPath(job, new Path("/Users/merqurius/Workspace/CS585Data/Transaction.txt"),TextInputFormat.class, TransactionMapper.class);
		 Path outputPath = new Path("/Users/merqurius/Workspace/CS585Data/out/");
//		 Path outputPath = new Path(args[2]);
		 FileOutputFormat.setOutputPath(job, outputPath);
		 outputPath.getFileSystem(conf).delete(outputPath);
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 }
}

