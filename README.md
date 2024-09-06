# BigDataAnalytics_DA-1

## Problem:
To create a MapReduce program that processes a file named 'stud.txt', which contains the scores of various students in a set of subjects.

## Code and Explaination:
Toppers_22MIS1006 is the main class of the program, which contains all the components of the MapReduce job, including the mapper, reducer, partitioner, and the main method.
### Mapper Part:
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
### Reducer part:
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
### Partitioner part:
```java
    public static class Partition extends Partitioner<Text, Text> {
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
- The Partition class extends Hadoop's Partitioner class and specifies that both the input and output key-value pairs will be of type
Text.
The reduce function is defined here takes three parameters:
  1. key: The key for the current data (in this case, the subject like "Mathematics").
  2. val: The value associated with the key (in this case, the student's name and score).
  3. num: The total number of reducers (num reducers are available for processing the data).
- The switch statement checks the value of the subject (key). Depending on the subject, it returns an integer that specifies which reducer the key-value pair should be sent to.
- If the subject does not match any of the predefined cases, it falls to the default case, which assigns it to reducer 9.
```java
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");

        Job job = Job.getInstance(conf, "Topper Determination");
        job.setJarByClass(Toppers_22MIS1006.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);

        job.setNumReduceTasks(10);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
- A new Hadoop configuration object is created in the variable name 'conf'.A new Job instance is created with the configuration settings conf. The job is given the name "Topper Determination".
- job.setJarByClass(Toppers_22MIS1006.class) specifies the class containing the main method that will be used to run the job. It ensures that the required .jar file containing all necessary classes is set properly.
- job.setInputFormatClass(KeyValueTextInputFormat.class) specifies the input format for the job. In this case, KeyValueTextInputFormat is used, which expects the input file to contain key-value pairs separated by a space.
- Then the Mapper, Partioner, and REducer classes are being set. The job.setNumReduceTasks(10) specifies that 10 reducers will be used.
- The data type of the output key and value from the reducer is being specified as Text.
- Next, the input and output paths are being set. Finally the job is submitted and the program waits for completion.
## Output:
![issue](https://github.com/user-attachments/assets/d3eb92e5-a401-4420-8125-195a6b84c502)
