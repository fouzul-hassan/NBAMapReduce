package DataAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MostScoringPlayerAnalysis {

    public static class ScoringMapper extends Mapper<LongWritable, Text, Text, PlayerScoreWritable> {
        private final Text teamKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",", -1);
            if (fields.length < 27) return;

            String player = fields[7].trim();
            String team = fields[8].trim();
            String description = (fields[3] + " " + fields[26]).toLowerCase();

            int points = 0;
            if (description.contains("3pt")) points = 3;
            else if (description.contains("2 pts") || description.contains("jump shot")) points = 2;
            else if (description.contains("free throw")) points = 1;
            else return;

            if (!player.isEmpty() && !team.isEmpty()) {
                teamKey.set(team);
                context.write(teamKey, new PlayerScoreWritable(player, points));
            }
        }
    }

    public static class ScoringCombiner extends Reducer<Text, PlayerScoreWritable, Text, PlayerScoreWritable> {
        @Override
        protected void reduce(Text team, Iterable<PlayerScoreWritable> scores, Context context) throws IOException, InterruptedException {
            Map<String, Integer> playerScores = new HashMap<>();
            for (PlayerScoreWritable score : scores) {
                playerScores.merge(score.getPlayerName(), score.getScore(), Integer::sum);
            }

            for (Map.Entry<String, Integer> entry : playerScores.entrySet()) {
                context.write(team, new PlayerScoreWritable(entry.getKey(), entry.getValue()));
            }
        }
    }

    public static class ScoringReducer extends Reducer<Text, PlayerScoreWritable, Text, IntWritable> {
        private final Map<String, Integer> teamScores = new HashMap<>();
        private final Map<String, PlayerScoreWritable> topPlayers = new HashMap<>();

        @Override
        protected void reduce(Text team, Iterable<PlayerScoreWritable> values, Context context) throws IOException, InterruptedException {
            int teamTotal = 0;
            Map<String, Integer> playerScores = new HashMap<>();

            for (PlayerScoreWritable val : values) {
                playerScores.merge(val.getPlayerName(), val.getScore(), Integer::sum);
                teamTotal += val.getScore();
            }

            String topPlayer = playerScores.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
            int topScore = playerScores.get(topPlayer);

            teamScores.put(team.toString(), teamTotal);
            topPlayers.put(team.toString(), new PlayerScoreWritable(topPlayer, topScore));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Most Scoring Player per Team:"), null);
            for (Map.Entry<String, PlayerScoreWritable> entry : topPlayers.entrySet()) {
                context.write(new Text(entry.getKey() + ": " + entry.getValue().getPlayerName()), new IntWritable(entry.getValue().getScore()));
            }

            context.write(new Text("\n🏆 Most Scored Team Overall:"), null);
            String topTeam = teamScores.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
            context.write(new Text(topTeam), new IntWritable(teamScores.get(topTeam)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Scoring Player Analysis");

        job.setJarByClass(MostScoringPlayerAnalysis.class);
        job.setMapperClass(ScoringMapper.class);
        job.setCombinerClass(ScoringCombiner.class);
        job.setReducerClass(ScoringReducer.class);
        job.setPartitionerClass(CustomPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PlayerScoreWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
