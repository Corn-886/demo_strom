package stream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.util.Map;

/**
 * 把数据保存到elasticsearch中，用于查询
 * Created by Corn on 2017/4/8.
 */
public class SaveBolt extends BaseRichBolt {
    OutputCollector m_collector;

    private static final Logger log = LogManager.getLogger(SaveBolt.class);

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        m_collector = collector;
    }

    public void execute(Tuple input) {
        System.out.println("save 节点接受的数据： " + input.getString(0));
        try {

            Client client = TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

            IndexResponse indexResponse = client
                    .prepareIndex()
                    .setIndex("test")
                    .setType("article").setSource(input.getString(0))
                    .get();
            client.close();
            System.out.println("执行结果"+indexResponse.isCreated());
        } catch (Exception e) {
            System.out.println("保存失败");
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("save"));
    }
}
