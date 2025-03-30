package DataAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MostScoringPlayerAnalysis {

    // Mapper
    public static class ScoringMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text teamPlayerKey = new Text();
        private final IntWritable pointValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",", -1);
            if (fields.length < 27) return;

            String player = fields[7].trim();    // PLAYER1_NAME
            String team = fields[8].trim();      // PLAYER1_TEAM_ABBREVIATION
            String homeDesc = fields[3].toLowerCase();
            String visitorDesc = fields[26].toLowerCase();
            String description = homeDesc + " " + visitorDesc;

            int points = 0;
            if (description.contains("3pt")) points = 3;
            else if (description.contains("2 pts") || description.contains("jump shot")) points = 2;
            else if (description.contains("free throw")) points = 1;
            else return;  // Skip non-scoring events

            if (!player.isEmpty() && !team.isEmpty()) {
                teamPlayerKey.set(team + "," + player);
                pointValue.set(points);
                context.write(teamPlayerKey, pointValue);
            }
        }
    }

    // Reducer
    public static class ScoringReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final Map<String, PlayerScore> teamTopScorer = new HashMap<>();
        private PlayerScore topScorerOverall = new PlayerScore("", "", 0); // NEW

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalPoints = 0;
            for (IntWritable val : values) {
                totalPoints += val.get();
            }

            context.write(new Text(String.format("%-30s", key.toString())), new IntWritable(totalPoints));

            String[] parts = key.toString().split(",", 2);
            if (parts.length != 2) return;

            String team = parts[0];
            String player = parts[1];

            // Track top scorer per team
            PlayerScore current = teamTopScorer.getOrDefault(team, new PlayerScore("", "", 0));
            if (totalPoints > current.score) {
                teamTopScorer.put(team, new PlayerScore(team, player, totalPoints));
            }

            // Track top scorer overall
            if (totalPoints > topScorerOverall.score) {
                topScorerOverall = new PlayerScore(team, player, totalPoints);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(""), new IntWritable()); // spacer
            context.write(new Text("Most Scoring Player Per Team:"), new IntWritable());

            for (Map.Entry<String, PlayerScore> entry : teamTopScorer.entrySet()) {
                PlayerScore ps = entry.getValue();
                context.write(new Text(String.format("%-5s: %-20s", ps.team, ps.player)), new IntWritable(ps.score));
            }

            context.write(new Text(""), new IntWritable()); // spacer
            context.write(new Text("🏆 Top Scoring Player Overall:"), new IntWritable());
            context.write(new Text(String.format("%-5s: %-20s", topScorerOverall.team, topScorerOverall.player)), new IntWritable(topScorerOverall.score));
        }

        private static class PlayerScore {
            String team;
            String player;
            int score;

            PlayerScore(String team, String player, int score) {
                this.team = team;
                this.player = player;
                this.score = score;
            }
        }
    }


    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MostScoringPlayerAnalysis <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Scoring Player Analysis");
        job.setJarByClass(MostScoringPlayerAnalysis.class);

        job.setMapperClass(ScoringMapper.class);
        job.setReducerClass(ScoringReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
