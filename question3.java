
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


  public class question3 {
		public static class question3Mapper extends
		Mapper<LongWritable, Text, Text, Text> {
			public void map(LongWritable key, Text value, Context context
	        ) throws IOException, InterruptedException {
				try {
				String[] str = value.toString().split(",");
				String cname= str[2];
				String position= str[4];
				if(str[4].equals("DATA SCIENTIST"))
				context.write(new Text(cname),new Text(position));
		           
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	

		public static class question3Reducer extends
		Reducer<Text, Text, NullWritable, Text> {
			private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

			public void reduce(Text key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {
				long count=0;
				String myvalue= "";
				String mysum= "";
					for (Text val : values) {
						count++;
					}
					myvalue= key.toString();
					mysum= String.format("%d", count);
					myvalue= myvalue + ',' + mysum;
					repToRecordMap.put(new Long(count), new Text(myvalue));
					if (repToRecordMap.size() > 1) {
					repToRecordMap.remove(repToRecordMap.firstKey());
							}
			}
			
					protected void cleanup(Context context) throws IOException,
					InterruptedException {
					for (Text t : repToRecordMap.values()) {
						// Output our five records to the file system with a null key
						context.write(NullWritable.get(), t);
						}
					}
			
		}
			
		public static void main(String[] args) throws Exception {
			
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Top record for largest amount spent");
		    job.setJarByClass(question3.class);
		    job.setMapperClass(question3Mapper.class);
		    job.setReducerClass(question3Reducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}
