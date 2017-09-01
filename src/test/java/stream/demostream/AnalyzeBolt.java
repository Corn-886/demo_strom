package stream.demostream;

import com.alibaba.fastjson.JSON;
import com.hankcs.hanlp.HanLP;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pojo.ArticlePojo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Corn on 2017/4/3.
 */
public class AnalyzeBolt extends BaseRichBolt {
    OutputCollector m_collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        m_collector = collector;
    }

    public Map<String, Integer> NameCountMap = new HashMap<String, Integer>();

    public void execute(Tuple input) {
        System.out.println("analyze接收到数据：" + input.getString(0));
        String data = input.getString(0);
        try {
            ArticlePojo a = JSON.parseObject(data, ArticlePojo.class);
            List<String> keywordList = HanLP.extractKeyword(a.getContent(), 5);
            a.setKeywords(keywordList.toString());
            List<String> sumary=HanLP.extractSummary(a.getContent(),3);
            a.setNote(sumary.toString());
            String a1 = JSON.toJSON(a).toString();
            System.out.println("analyze 处理完的数据： "+a1);
            m_collector.emit(input, new Values(a1));//发送
            m_collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("处理失败的数据："+input.getString(0));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("analyze"));
    }
}
