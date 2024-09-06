# BigDataAnalytics_DA-1

##Problem:
To create a MapReduce program that processes a file named 'stud.txt', which contains the scores of various students in a set of subjects.

## Code and Explaination:
Toppers_22MIS1006 is the main class of the program, which contains all the components of the MapReduce job, including the mapper, reducer, partitioner, and the main method.
```java
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Toppers_22MIS1006 {
    public static class Map extends Mapper<Text, Text, Text, Text>{
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
```
- The Map class extends Hadoop's Mapper class and specifies that both the input and output key-value pairs will be of type Text.
  The map function is defined here takes three parameters:
  1. key: The input key, which is the subject (e.g., "Mathematics").
  2. value: The input value, which is the student name and score (e.g., "Rohan 85").
  3. context: The Context object, which allows the map function to emit the key-value pairs for the reducer and interact with the Hadoop framework.
- The map function outputs the input key-value pair as is, without any transformation. The key is the subject name, and the value is the student name and score. The output will be used by the partitioner and reducer later on.
  
```java
    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> val, Context context) throws IOException, InterruptedException {
            String name = "";
            int mark = 0;
            for (Text t : val) {
                String str = t.toString();
                String[] s = str.split(" ");
                int score = Integer.parseInt(s[1]);
                if (score > mark) {
                    mark = score;
                    name = s[0];
                }
            }
            String result = name + " " + mark;
            context.write(key, new Text(result));
        }
    }
```
- The Reduce class extends Hadoop's Reducer class and specifies that both the input and output key-value pairs will be of type Text.
  The reduce function is defined here takes three parameters:
  1. key: The input key, which is the subject (e.g., "Mathematics").
  2. value: The input value, which is the student name and score (e.g., ["Rohan 85","Riya 92"]).
  3. context: The Context object, which allows the map function to emit the key-value pairs for the reducer and interact with the Hadoop framework.
- Variables mark and name are used to track the highest score and the name of the top student.
- For each subject, it iterates over the list of students and their scores, comparing each score to the current highest. If a higher score is found, it updates the top score and student's name.
- After processing all scores, the reducer outputs the subject along with the name and score of the top-performing student.
```java
    public static class Partion extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text val, int num) {
            switch (key.toString()) {
                case "Mathematics": return 0;
                case "Physics": return 1;
                case "Chemistry": return 2;
                case "Biology": return 3;
                case "English": return 4;
                case "History": return 5;
                case "Geography": return 6;
                case "CS": return 7;
                case "Economics": return 8;
                default: return 9;
            }
        }
    }
```
```java
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");

        Job job = Job.getInstance(conf, "Topper Determination");
        job.setJarByClass(Toppers_22MIS1006.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partion.class);

        job.setNumReduceTasks(10);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
