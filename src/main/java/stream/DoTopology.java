package stream;

import kafka.MessageScheme;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * 执行拓扑
 * Created by Corn on 2017/4/8.
 */
public class DoTopology {
    private static String ZkHosts = "192.168.184.128:2181";
    private static String ZkKafka = "192.168.184.128:9092";
    private static String topic = "sym_graduate";//kafka接收主题
    private static String zkRoot = "";//zookeeper设置文件中的配置，如果zookeeper配置文件中设置为主机名：端口号 ，该项为空
    private static String spoutId = "su";

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        BrokerHosts brokerHosts = new ZkHosts(ZkHosts);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        Config conf = new Config();
        conf.setDebug(false);//kafka 配置作为数据源
        Map<String, String> map = new HashMap<String, String>();
        map.put("metadata.broker.list", ZkKafka);
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", map);
        conf.put("topic", "receiver");
        conf.put("forceFromStart","false");
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("soup", new KafkaSpout(spoutConfig), 1);
        builder.setBolt("analyze", new AnalyzeBolt(), 5).shuffleGrouping("soup");
        builder.setBolt("save", new SaveBolt(), 5).shuffleGrouping("analyze");

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            //提交本地集群
            cluster.submitTopology("test", conf, builder.createTopology());
            //等待6s之后关闭集群
            Thread.sleep(6000000);
            //关闭集群
            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology("test", conf, builder.createTopology());
        }
    }
}

