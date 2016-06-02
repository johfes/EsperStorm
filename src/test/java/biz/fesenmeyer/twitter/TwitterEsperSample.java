package biz.fesenmeyer.twitter;

import org.tomdz.storm.esper.EsperBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import biz.fesenmeyer.twitter.*;

public class TwitterEsperSample
{
    public static void main(String[] args)
    {

        String consumerKey = "421HJxi0ZGMqGBMVK57u1yNKK";
        String consumerSecret = "c7yPasfGR4adIexeLY0TRKRTedLf5jCLbyrLyjwVYdaluyah6e";
  		
        String accessToken = "3019676494-o733TZQJu0Gx1le0ZCKP1sn2zv6KzPGjbxmsLo6";
        String accessTokenSecret = "G0FacVvX4RFEGXLEzNZTTF8anvcA9Ptf50GTlGZHrqZYH";
		
        String[] keyWords = {};

        TopologyBuilder builder = new TopologyBuilder();
        TwitterSampleSpout spout = new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords);
        EsperBolt bolt = new EsperBolt.Builder()
                                      .inputs().aliasComponent("split").withFields("word").ofType(String.class).toEventType("Word")
                                      .outputs().onDefaultStream().emit("word")
                                      .statements().add("select * from Word.win:length_batch(1) WHERE word In "+ 
                                      "('Waihopai', 'INFOSEC','IW', 'IS', 'Privacy', 'InfoSec', 'Reno', 'Compsec', 'Firewalls', 'ISS', 'Passwords', 'Hackers', 'Encryption', 'Espionage', 'USDOJ', 'NSA', 'CIA', 'S/Key', 'SSL', 'FBI', 'USSS', 'Defcon', 'Military', 'Undercover', 'NCCS', 'Mayfly', 'PGP', 'PEM', 'RSA', 'Perl-RSA', 'MSNBC', 'bet', 'AOL', 'CIS', 'CBOT', 'AIMSX', 'STARLAN', '3B2', 'BITNET', 'COSMOS', 'DATTA', 'E911', 'FCIC', 'HTCIA', 'IACIS', 'UT/RUS', 'JANET', 'JICC')")
                                      .build();

        builder.setSpout("spout", spout);
        builder.setBolt("split", new SplitTweet()).shuffleGrouping("spout");
        builder.setBolt("analyze", bolt).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
    }
}
