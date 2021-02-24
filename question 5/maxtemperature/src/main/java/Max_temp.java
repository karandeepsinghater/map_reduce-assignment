
import java.io.IOException;
import java.util.StringTokenizer;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





public class Max_temp {



    public static class Map extends
            Mapper<LongWritable, Text, Text, IntWritable> {



        Text k= new Text();


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            StringTokenizer tokenizer = new StringTokenizer(line," ");

            while (tokenizer.hasMoreTokens()) {

                String year= tokenizer.nextToken();
                k.set(year);

                String temp= tokenizer.nextToken().trim();

                int v = Integer.parseInt(temp);

                context.write(k,new IntWritable(v));
            }
        }
    }

    //Reducer


    public static class Reduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            

            //Defining a local variable maxtemp of type int
            int maxtemp=0;
            for(IntWritable it : values) {

                //Defining a local variable temperature of type int which is taking all the temperature
                int temperature= it.get();
                if(maxtemp<temperature)
                {
                    maxtemp =temperature;
                }
            }

            //Finally the output is collected as the year and maximum temperature corresponding to that year
            context.write(key, new IntWritable(maxtemp));
        }

    }

    //Driver


        public static void main(String[] args) throws Exception {

        //reads the default configuration of cluster from the configuration xml files

        Configuration conf = new Configuration();

        //Initializing the job with the default configuration of the cluster


        Job job = new Job(conf, "Max_temp");

        //Assigning the driver class name

        job.setJarByClass(Max_temp.class);

        //Defining the mapper class name

        job.setMapperClass(Map.class);

        //Defining the reducer class name

        job.setReducerClass(Reduce.class);

        //Defining the output key class for the final output i.e. from reducer

        job.setOutputKeyClass(Text.class);

        //Defining the output value class for the final output i.e. from reducer

        job.setOutputValueClass(IntWritable.class);

        //Defining input Format class which is responsible to parse the dataset into a key value pair

        job.setInputFormatClass(TextInputFormat.class);

        //Defining output Format class which is responsible to parse the final key-value output from MR framework to a text file into the hard disk

        job.setOutputFormatClass(TextOutputFormat.class);

        //setting the second argument as a path in a path variable

        Path outputPath = new Path(args[1]);

        //Configuring the input/output path from the filesystem into the job

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //deleting the output path automatically from hdfs so that we don't have delete it explicitly

        outputPath.getFileSystem(conf).delete(outputPath);

        //exiting the job only if the flag value becomes false

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
