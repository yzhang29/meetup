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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuan.storm.proto.MeetupProtos;
/**
 * Created by yzhang29 on 5/1/15.
 */
import java.io.IOException;
import java.util.Map;

import static yuan.storm.proto.MeetupProtos.*;

/**
 * A bolt that parses the meetup into json feeds.
 */
public class ParseMeetupFeedsBolt extends BaseRichBolt{

    // To output tuples from this bolt to the count bolt
    OutputCollector collector;
    private static ObjectMapper mapper = new ObjectMapper();
    private final Logger logger = LoggerFactory.getLogger(ParseMeetupFeedsBolt.class);


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
        String feed = tuple.getString(0);
        System.out.println("FEEDLA" + feed);
        MeetupEvent.Builder eventBuilder = MeetupEvent.newBuilder();
        try {
            JsonNode meetupData = mapper.readTree(feed);
            if(meetupData.get("status").asText().equals("upcoming")) {
                eventBuilder.setId(meetupData.path("id").asText());
                eventBuilder.setCategory(meetupData.path("group").path("category").path("shortname").asText());
                eventBuilder.setTime(meetupData.get("time").longValue());
                collector.emit(new Values(eventBuilder.build()));
            }
        } catch (IOException e) {
            logger.error("Error parsing json stream" + e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // tell storm the schema of the output tuple for this spout
        declarer.declare(new Fields("meetup-proto"));
    }
}
