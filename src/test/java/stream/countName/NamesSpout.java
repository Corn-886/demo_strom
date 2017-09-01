package stream.countName;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**数据源
 * Created by Corn on 2017/3/30.
 */
public class NamesSpout extends BaseRichSpout {
    SpoutOutputCollector m_collector;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        m_collector = collector;
    }

    public void nextTuple() {
        final String[] names = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
        final Random rand = new Random();
        final String name = names[rand.nextInt(names.length)];

        Utils.sleep(10);
        m_collector.emit(new Values(name));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("name"));
    }
}
