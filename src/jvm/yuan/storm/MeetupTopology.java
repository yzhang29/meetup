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

class MeetupTopology
{
  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */

    //*********************************************************************
    // Complete the Topology.
    // Part 0: attach the tweet spout to the topology - parallelism of 1
      builder.setSpout("meetup-spout", new MeetupSpout(), 1);
    // Part 1: // attach the parse tweet bolt, parallelism of 10 (what grouping is needed?)
      builder.setBolt("parse-meetup-bolt", new ParseMeetupFeedsBolt(), 10).shuffleGrouping("meetup-spout");
    // Part 3: attach the report bolt, parallelism of 1 (what grouping is needed?)
      builder.setBolt("redis-bolt", new RedisWriterBolt(), 10).shuffleGrouping("parse-meetup-bolt");
    // Submit and run the topology.


    //*********************************************************************

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("meetup", conf, builder.createTopology());

    }
  }
}
