package stream.countName;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Corn on 2017/3/30.
 */
public class ExclamationBolt extends BaseRichBolt {
    OutputCollector m_collector;
    public Map<String, Integer> NameCountMap = new HashMap<String, Integer>();

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        m_collector = collector;
    }

    public void execute(Tuple input) {
        // 第一步，统计计算
        Integer value = 0;
        if (NameCountMap.containsKey(input.getString(0))) {
            value = NameCountMap.get(input.getString(0));
        }
        NameCountMap.put(input.getString(0), ++value);

        // 第二步，输出
        System.out.println(input.getString(0) + "!!!");
        System.out.println(value);

        m_collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("exclaim"));
    }
}
