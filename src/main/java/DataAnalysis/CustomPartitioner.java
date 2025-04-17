package DataAnalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, PlayerScoreWritable> {
    @Override
    public int getPartition(Text key, PlayerScoreWritable value, int numPartitions) {
        return Math.abs(key.hashCode() % numPartitions);
    }
}