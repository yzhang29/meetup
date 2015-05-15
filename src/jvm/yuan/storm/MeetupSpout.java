package yuan.storm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.Response;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */
public class MeetupSpout extends BaseRichSpout
{
  // To output tuples from spout to the next stage bolt
  SpoutOutputCollector collector;

  // Shared queue for getting buffering tweets received
  LinkedBlockingQueue<String> queue = null;

  public void getMeetupStream(final LinkedBlockingQueue<String> eventQueue) {
    AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    asyncHttpClient.prepareGet("http://stream.meetup.com/2/open_events").execute(new AsyncCompletionHandler<Response>() {
      @Override
      public Response onCompleted(Response response) throws Exception {
        return response;
      }

      @Override
      public STATE onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
        eventQueue.add(new String(content.getBodyPartBytes()));
        return STATE.CONTINUE;
      }
    });
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector)
  {
    // create the buffer to block tweets
    queue = new LinkedBlockingQueue<>(1000);
    getMeetupStream(queue);
    // save the output collector for emitting tuples
    collector = spoutOutputCollector;

  }

  @Override
  public void nextTuple() 
  {
    // try to pick a tweet from the buffer
    String ret = queue.poll();

    // if no tweet is available, wait for 50 ms and return
    if (ret==null) 
    {
      Utils.sleep(1000);
      return;
    }

    // now emit the tweet to next stage bolt
    collector.emit(new Values(ret));
  }

  /**
   * Component specific configuration
   */
  @Override
  public Map<String, Object> getComponentConfiguration() 
  {
    // create the component config 
    Config ret = new Config();
 
    // set the parallelism for this spout to be 1
    ret.setMaxTaskParallelism(1);

    return ret;
  }    

  @Override
  public void declareOutputFields(
      OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet'
    outputFieldsDeclarer.declare(new Fields("meetup"));
  }
}
