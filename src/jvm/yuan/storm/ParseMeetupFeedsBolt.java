package yuan.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Bolt;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.json.JSONObject;
import yuan.storm.proto.MeetupProtos;
/**
 * Created by yzhang29 on 5/1/15.
 */
import java.util.Map;

import static yuan.storm.proto.MeetupProtos.*;

/**
 * A bolt that parses the meetup into json feeds.
 */
public class ParseMeetupFeedsBolt extends BaseRichBolt{

    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    @Override
    public void prepare(
            Map map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {
        // save the output collector for emitting tuples
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        // get the 1st column 'tweet' from tuple
        String feed = tuple.getString(0);
        JSONObject obj = new JSONObject(feed);
        String n = obj.getString("name");
        int a = obj.getInt("age");
        System.out.println(n + " " + a);
        MeetupEvent.Builder eventBuilder = MeetupEvent.newBuilder();

        // for each token/word, emit it
        for (String token: tokens) {
            collector.emit(new Values(token));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet-word'
        declarer.declare(new Fields("tweet-word"));
    }
}
