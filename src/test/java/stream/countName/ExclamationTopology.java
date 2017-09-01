package stream.countName;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by Corn on 2017/3/30.
 */
public class ExclamationTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder =  new TopologyBuilder();
        builder.setSpout("name", new NamesSpout(), 5);
        builder.setBolt("exclaim", new ExclamationBolt(), 5).shuffleGrouping("name");

        Config conf =new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
