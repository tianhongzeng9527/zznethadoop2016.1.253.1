import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.Constants;
import usrhandle.Category;
import usrhandle.SensitiveWord;
import usrhandle.UserDataInformation;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static String TIME_STRING = "time";
    private static String time = "time";
    private static String calculate_word_distance_url = "calculate_word_distance_url";
    private static String CALCULATE_WORD_DISTANCE_URL_STRING = "calculate_word_distance_url";
    private static String category_get_url = "category_get_url";
    private static String CATEGORY_GET_URL = "category_get_url";
    private static String sensitiveWord_get_url = "sensitiveWord_get_url";
    private static String SENSITIVE_WORD_GET_URL = "sensitiveWord_get_url";

    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable zero = new IntWritable(0);

        public void configure(JobConf job) {
            time = job.get(TIME_STRING);
//            calculate_word_distance_url = job.get(CALCULATE_WORD_DISTANCE_URL_STRING);
//            category_get_url = job.get(CATEGORY_GET_URL);
//            sensitiveWord_get_url = job.get(SENSITIVE_WORD_GET_URL);
            calculate_word_distance_url = "http://192.168.1.106:8080/zz_nlp/wordsDistance?word1=%s&word2=%s";
            category_get_url = "http://192.168.1.106:8080/public_behavior/api/behavior.do";
            sensitiveWord_get_url = "http://192.168.1.106:8080/public_behavior/api/sensitive.do";
        }

        private void handlerRightTimeInformation(UserDataInformation userDataInformation, String line, OutputCollector<Text, IntWritable> output) throws IOException {
            if (userDataInformation.isRightTime()) {
                if (!userDataInformation.isNormalMessage) {
                    output.collect(new Text(Constants.WRONG_FORMAT + line), zero);
                    return;
                } else {
                    try {
                        userDataInformation.commonClassify();
                    } catch (Exception e) {
                        output.collect(new Text(Constants.UNABLE_CATEGORY + line), zero);
                        return;
                    }
                    userDataInformation.sensitiveClassify();
                    List<String> sensitiveId = userDataInformation.getSensitiveId();
                    if (sensitiveId != null && sensitiveId.size() > 0) {
                        for (String s : sensitiveId) {
                            output.collect(new Text(userDataInformation.toString()), new IntWritable(Integer.valueOf(s)));
                        }
                    } else {
                        output.collect(new Text(userDataInformation.toString()), new IntWritable(1));
                        if (userDataInformation.getReturnType() == Constants.RETURN_TYPE_NORMAL) {
                            output.collect(new Text(userDataInformation.getURLCategory()), new IntWritable(1));
                        }
                    }
                }
            }
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            if (Category.categorySuperId == null) {
                Category category = new Category(category_get_url);
                category.init();
            }
            if (SensitiveWord.sensitiveWordCorrespondId == null) {
                SensitiveWord sensitiveWord = new SensitiveWord(sensitiveWord_get_url);
                sensitiveWord.init();
            }
            if (line.indexOf(time) == 0) {
                UserDataInformation userDataInformation;
                try {
                    userDataInformation = new UserDataInformation(line, calculate_word_distance_url);
                } catch (Exception e) {
                    output.collect(new Text(Constants.PROGRAM_EXCEPTION + line), zero);
                    return;
                }
                handlerRightTimeInformation(userDataInformation, line, output);
            }
        }
    }

    static class MyMultipleOutputFormat extends MultipleTextOutputFormat<Text, IntWritable> {
        @Override
        protected String generateFileNameForKeyValue(Text key, IntWritable value, String name) {
            if (key.toString().contains(Constants.UNABLE_CATEGORY)) {
                return Constants.UNABLE_CATEGORY + name;
            } else if (key.toString().contains(Constants.PROGRAM_EXCEPTION)) {
                return Constants.PROGRAM_EXCEPTION + name;
            } else if (key.toString().contains(Constants.UNABLE_CONNECT_URL)) {
                return Constants.UNABLE_CONNECT_URL + name;
            } else if (key.toString().contains(Constants.WRONG_FORMAT)) {
                return Constants.WRONG_FORMAT;
            }
            String[] split = key.toString().trim().split(Constants.SEPARATOR);
            if (split.length >= Constants.NORMAL_CATEGORY_LENGTH) {
                return "normal" + name;
            } else if (split.length == Constants.SENSITIVE_CATEGORY_LENGTH) {
                return "sensitive" + name;
            } else if (split.length == Constants.URL_CATEGORY_LENGTH) {
                return "urlCategory" + name;
            } else {
                return "noise" + name;
            }
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String[] splits = key.toString().trim().split(Constants.SEPARATOR);
            if (splits.length >= Constants.NORMAL_CATEGORY_LENGTH) {
                int sum = 0;
                while (values.hasNext()) {
                    sum += values.next().get();
                    logger.info(key.toString());
                }
                output.collect(key, new IntWritable(sum));
            } else if (splits.length == Constants.SENSITIVE_CATEGORY_LENGTH) {
                while (values.hasNext()) {
                    output.collect(key, values.next());
                }
            } else {
                output.collect(key, values.next());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("zznet");
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(CombineTextInputFormat.class);
//        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(MyMultipleOutputFormat.class);
        conf.set("mapred.textoutputformat.separator", Constants.SEPARATOR);
        conf.set("mapreduce.input.fileinputformat.input.dir.recursive", String.valueOf(true));
        String inputTime = args[0].substring(args[0].lastIndexOf("/") + 1, args[0].length());
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Date dt = df.parse(inputTime);
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(dt);
        rightNow.add(Calendar.DAY_OF_MONTH, 1);
        Date dt1 = rightNow.getTime();
        File file = new File(args[0].substring(0, args[0].lastIndexOf("/")) + "/" + df.format(dt1) + "/" + "00");
        if (file.exists())
            FileInputFormat.setInputPaths(conf, new Path(args[0]), new Path(args[0].substring(0, args[0].lastIndexOf("/")) + "/" + df.format(dt1) + "/" + "00"));
        else
            FileInputFormat.setInputPaths(conf, new Path(args[0]));
        String s = df.format(dt);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.set(TIME_STRING, s);
//        conf.set(CALCULATE_WORD_DISTANCE_URL_STRING, args[2]);
//        conf.set(CATEGORY_GET_URL, args[3]);
//        conf.set(SENSITIVE_WORD_GET_URL, args[4]);
        JobClient.runJob(conf);
    }

}