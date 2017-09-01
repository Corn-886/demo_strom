package stream;

import com.alibaba.fastjson.JSON;
import com.hankcs.hanlp.HanLP;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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
 * 文章分析拓扑节点
 * Created by Corn on 2017/4/8.
 */
public class AnalyzeBolt extends BaseRichBolt {
    OutputCollector m_collector;
    private static final long serialVersionUID = -2647123143398352022L;

    private static final Logger log = LogManager.getLogger(BaseRichBolt.class);

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        m_collector = collector;
    }

    public Map<String, Integer> NameCountMap = new HashMap<String, Integer>();

    /**
     * 把string 转为对象，处理，处理完成以后再转为string发送到下一节点
     * @param input
     */
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
