package biz.fesenmeyer.example.randomSentence;

import static backtype.storm.utils.Utils.tuple;

import org.tomdz.storm.esper.EsperBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class EsperSample {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {

        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .inputs().aliasComponent("split").withFields("word").ofType(String.class).toEventType("Word")
                                           .outputs().onDefaultStream().emit("word")
                                           .statements().add("select word from Word.win:length_batch(1) WHERE word='dangerousWord'")
                                           .build();
        
		//Used to build the topology
	    TopologyBuilder builder = new TopologyBuilder();
	    //Add the spout, with a name of 'spout'
	    //and parallelism hint of 5 executors
	    builder.setSpout("spout", new RandomTextSpout(), 5);
	    //Add the counter, with a name of 'count'
	    //and parallelism hint of 12 executors
	    //fieldsgrouping subscribes to the split bolt, and
	    //ensures that the same word is sent to the same instance (group by field 'word')
	    builder.setBolt("split", new SplitText(), 8).shuffleGrouping("spout");
	    builder.setBolt("bolt", esperBolt, 12).fieldsGrouping("split", new Fields("word"));

	    //new configuration
	    Config conf = new Config();
	    conf.setDebug(true);

	    //If there are arguments, we are running on a cluster
	    if (args != null && args.length > 0) {
	      //parallelism hint to set the number of workers
	      conf.setNumWorkers(3);
	      //submit the topology
	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    }
	    //Otherwise, we are running locally
	    else {
	      //Cap the maximum number of executors that can be spawned
	      //for a component to 3
	      conf.setMaxTaskParallelism(3);
	      //LocalCluster is used to run locally
	      LocalCluster cluster = new LocalCluster();
	      //submit the topology
	      cluster.submitTopology("text-analysis", conf, builder.createTopology());
	      //sleep
	      Thread.sleep(10000);
	      //shut down the cluster
	      cluster.shutdown();
	    }
	}

}
