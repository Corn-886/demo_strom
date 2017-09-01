package stream.demostream;

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
import stream.kafka.MessageScheme;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Corn on 2017/4/3.
 */
public class ExcuteTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        //配置zookeeper 主机:端口号
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
        //接收消息队列的主题
        String topic = "sym_graduate";
        //zookeeper设置文件中的配置，如果zookeeper配置文件中设置为主机名：端口号 ，该项为空
        String zkRoot = "";
        //任意
        String spoutId = "zhou";
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        //设置如何处理kafka消息队列输入流
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        Config conf = new Config();
        //不输出调试信息
        conf.setDebug(false);
        //设置一个spout task中处于pending状态的最大的tuples数量
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        Map<String, String> map = new HashMap<String, String>();
        // 配置Kafka broker地址
        map.put("metadata.broker.list", "localhost:9092");
        // serializer.class为消息的序列化类
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", map);
        // 配置KafkaBolt生成的topic
        conf.put("topic", "receiver");
        TopologyBuilder builder = new TopologyBuilder();

       builder.setSpout("soup", new KafkaSpout(spoutConfig), 1);
//        builder.setSpout("soup", new ArticleSpout(), 5);
        builder.setBolt("analyze", new AnalyzeBolt(), 5).shuffleGrouping("soup");
        builder.setBolt("save", new SaveBolt(), 5).shuffleGrouping("analyze");

        if(args.length==0){
            LocalCluster cluster = new LocalCluster();
            //提交本地集群
            cluster.submitTopology("sym_graduate", conf, builder.createTopology());
            //等待6s之后关闭集群
           Thread.sleep(600000000);
            //关闭集群
            cluster.shutdown();
        }else{
        StormSubmitter.submitTopology("sym_graduate", conf, builder.createTopology());}
    }
}

