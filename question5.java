
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




  public class question5 {
		public static class question3Mapper extends
		Mapper<LongWritable, Text, Text, Text > {
			public void map(LongWritable key, Text value, Context context
	        ) throws IOException, InterruptedException {
				try {
				String[] str = value.toString().split(",");
				String position= str[4];
				String year = str[7];
				context.write(new Text(position),new Text(year));
		           
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
				String value="";
					for (Text val : values) {
						count++;
						value=val.toString();
					}
					myvalue= key.toString();
					mysum= String.format("%d", count);
					myvalue= myvalue + ',' + mysum + ',' +value;
					repToRecordMap.put(new Long(count), new Text(myvalue));
					if (repToRecordMap.size() > 10) {
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
		
		
		public static class CaderPartitioner extends
		   Partitioner < Text, Text >
		   {
		      @Override
		      public int getPartition(Text key, Text value, int numReduceTasks)
		      {
		        String a=value.toString(); 
		         if(a.equals("2011"))
		         {
		            return 0;
		         }
		         else if(a.equals("2012"))
		         {
		            return 1;
		         }
		         else if(a.equals("2013"))
		         {
		            return 2;
		         }
		         else if(a.equals("2014"))
		         {
		            return 3;
		         }
		         else
		         {
		        	return 4;
		         }
		        	 
		         
		      }
		      
		   }
		   
			
		public static void main(String[] args) throws Exception {
			
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Top record for largest amount spent");
		    job.setJarByClass(question5.class);
		    job.setMapperClass(question3Mapper.class);
		    job.setReducerClass(question3Reducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setPartitionerClass(CaderPartitioner.class);
		    job.setNumReduceTasks(5);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}
