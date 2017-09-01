package stream.demostream;

import com.alibaba.fastjson.JSON;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import pojo.ArticlePojo;

import java.util.Map;

/**
 * Created by Corn on 2017/4/3.
 */
public class ArticleSpout extends BaseRichSpout {
    SpoutOutputCollector m_collector;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        m_collector = collector;
    }

    public void nextTuple() {
//        final String[] names = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
//        final Random rand = new Random();
//        final String name = names[rand.nextInt(names.length)];
        ArticlePojo art=new ArticlePojo("test","2016-5-1","test,sss","test","ssss");
       String a1= JSON.toJSON(art).toString();
        Utils.sleep(100);
        m_collector.emit(new Values(a1));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("article"));
    }
}
