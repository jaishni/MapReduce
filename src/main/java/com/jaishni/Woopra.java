package com.jaishni;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Simple MapReduce program that reads in json formatted data
 * and returns a result of the 100 most recent visitors grouped by "pid",
 * and ordered by most recent timestamp.
 *
 * sample input:
 * {
 *    \"continent\":\"NA\",
 *    \"date\":\"2017-03-31\",
 *    \"country\":\"CA\",
 *    \"pid":\"b0rwnatcqjbg",
 *
 *
 * sample output:
 * b0rwnatcqjbg	1491116371836
 * k6vapa6smyud	1491116291575
 * 1zrqzedy4hda	1491116180719
 * qsrcfnxkeizu	1491116153015
 * 1ptckfdkhmfb	1491116149699
 *
 * @author Jaishni Govender
 *
 */

public class Woopra {

    private Logger logger = Logger.getLogger(Woopra.class);

    /**
     * The number of records to be returned
     */
    final static int N = 100;

    /**
     * The json key that maps to the user id
     */
    final static String PID = "pid";

    /**
     * The json array that contains the list of timestamps associated with a user (pid)
     */
    final static String ACTIONS = "actions";

    /**
     * The json key that maps to the user timestamps
     */
    final static String TIME = "time";


    /**
     * The mapper class
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        final Map<String, Long> map = new HashMap();

        @Override
        public void map(final LongWritable key, final Text value, final Context context) {
            final Logger logger = Logger.getLogger(Woopra.MyMapper.class);
            String pid;
            long maxTimestamp;

            try {
               final String[] eventArray = value.toString().split("\\n");

                for (final String record : eventArray) {
                    final JSONObject jsonRecord = new JSONObject(record);
                    final JSONArray actionsArray =jsonRecord.getJSONArray(ACTIONS);

                    pid = jsonRecord.get(PID).toString();
                    maxTimestamp = getMostRecentTimestamp(actionsArray);

                    if (map.containsKey(pid)) {
                        if (map.get(pid) < maxTimestamp) {
                            map.put(pid, maxTimestamp);
                        }
                    } else {
                        map.put(pid, maxTimestamp);
                    }
                }
            } catch(final JSONException ex) {
                logger.error(ex.getMessage());
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {

            for (final String pid : map.keySet()) {
                context.write(new Text(pid), new LongWritable(map.get(pid)));
            }
        }

        /**
         * Method that reads in a Json array of timestamps and returns the most recent one
         * @param actionsArray
         * @return maxTimestamp

         */
        private long getMostRecentTimestamp (final JSONArray actionsArray) {
            long maxTimestamp = 0L;
            long currentTimestamp;

            for (int i = 0; i < actionsArray.length(); i++) {
                final JSONObject actions = (JSONObject) actionsArray.get(i);
                currentTimestamp = actions.getLong(TIME);

                if (currentTimestamp > maxTimestamp) {
                    maxTimestamp = currentTimestamp;
                }
            }

            return maxTimestamp;
        }
    }


    /**
     * The reducer class
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        final Logger logger = Logger.getLogger(Woopra.MyReducer.class);
        final TreeMap<String, Long> treeMap = new TreeMap<String, Long>(Collections.reverseOrder());

        @Override
        public void reduce(final Text key, final Iterable<LongWritable> values, final Context context) {
            long maxTimestamp = 0;
            long currentTimestamp = 0;

            for (final LongWritable value : values) {
                currentTimestamp = value.get();

                if (currentTimestamp > maxTimestamp) {
                    maxTimestamp = currentTimestamp;
                }
            }

            treeMap.put(maxTimestamp + key.toString(), maxTimestamp);
        }

        @Override
        public void cleanup(final Context context) throws IOException, InterruptedException {

            int counter = 0;
            int outputRecordCount = N;

            // if we have few result records than the number of records requested output everything
            if (treeMap.size() < N) {
                outputRecordCount = treeMap.size();
                logger.info("The number of records requested exceeds the record count in the input data.");
            }

            while (counter < outputRecordCount) {
                counter += 1;
                java.util.Map.Entry<String, Long> mapEntry = treeMap.firstEntry();
                context.write(new Text(mapEntry.getKey().split(mapEntry.getValue().toString())[1]), new LongWritable(mapEntry.getValue()));
                treeMap.remove(treeMap.firstKey());
            }
        }
    }

    public static void main(String[] args) throws Exception {

        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Woopra.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);

        if (status) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

}
