package biz.fesenmeyer.twitter;

import java.text.BreakIterator;
import java.util.Map;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class SplitTweet implements IRichBolt {
	   private OutputCollector collector;
	   
	   @Override
	//Execute is called to process tuples
	  public void execute(Tuple tuple) {
	    //Get the sentence content from the tuple
		      Status tweet = (Status) tuple.getValueByField("tweet");
		      String text = tweet.getText();
	    //An iterator to get each word
	    BreakIterator boundary=BreakIterator.getWordInstance();
	    //Give the iterator the tweet
	    boundary.setText(text);
	    //Find the beginning first word
	    int start=boundary.first();
	    //Iterate over each word and emit it to the output stream
	    for (int end=boundary.next(); end != BreakIterator.DONE; start=end, end=boundary.next()) {
	      //get the word
	      String word=text.substring(start,end);
	      //If a word is whitespace characters, replace it with empty
	      word=word.replaceAll("\\s+","");
	      //if it's an actual word, emit it
	      if (!word.equals("")) {
	        collector.emit(new Values(word));
	      }
	    }
	  }

	  //Declare that emitted tuples will contain a word field
	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("word"));
	  }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
