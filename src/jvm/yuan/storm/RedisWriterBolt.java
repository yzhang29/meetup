package yuan.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
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

import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuan.storm.proto.MeetupProtos;

import static yuan.storm.proto.MeetupProtos.*;

/**
 * A bolt that prints the word and count to redis
 */
public class RedisWriterBolt extends BaseRichBolt
{
  // place holder to keep the connection to redis
  transient RedisConnection<String,String> redis;
  private final Logger logger = LoggerFactory.getLogger(RedisWriterBolt.class);


  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // instantiate a redis connection
    RedisClient client = new RedisClient("localhost",6379);

    // initiate the actual connection
    redis = client.connect();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // access the first column 'word'
    MeetupEvent meetupEvent = (MeetupEvent) tuple.getValue(0);
    logger.info("meetupstream--got it" + meetupEvent.toString());
    System.out.print("hahahah" + meetupEvent.toString());
    redis.set(meetupEvent.getId(), meetupEvent.toString());

    // publish the word count to redis using word as the key
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
