package DataAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PlayerScoreWritable implements Writable {
    private Text playerName;
    private IntWritable score;

    public PlayerScoreWritable() {
        this.playerName = new Text();
        this.score = new IntWritable();
    }

    public PlayerScoreWritable(String playerName, int score) {
        this.playerName = new Text(playerName);
        this.score = new IntWritable(score);
    }

    public void set(String playerName, int score) {
        this.playerName.set(playerName);
        this.score.set(score);
    }

    public String getPlayerName() {
        return playerName.toString();
    }

    public int getScore() {
        return score.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        playerName.write(out);
        score.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        playerName.readFields(in);
        score.readFields(in);
    }
}
