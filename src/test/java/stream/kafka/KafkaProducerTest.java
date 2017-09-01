package stream.kafka;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by  on 2017/6/8.
 */
public class KafkaProducerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //The "all" setting we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
        //“所有”设置将导致记录的完整提交阻塞，最慢的，但最持久的设置。
        props.put("acks", "all");
        //如果请求失败，生产者也会自动重试，即使设置成0 the producer can automatically retry.
        props.put("retries", 0);

        //The producer maintains buffers of unsent records for each partition.
        props.put("batch.size", 16384);
        //默认立即发送，这里这是延时毫秒数
        props.put("linger.ms", 1);
        //生产者缓冲大小，当缓冲区耗尽后，额外的发送调用将被阻塞。时间超过max.block.ms将抛出TimeoutException
        props.put("buffer.memory", 33554432);
        //The key.serializer and value.serializer instruct how to turn the key and value objects the user provides with their ProducerRecord into bytes.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建kafka的生产者类
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        //生产者的主要方法
        // close();//Close this producer.
        //   close(long timeout, TimeUnit timeUnit); //This method waits up to timeout for the producer to complete the sending of all incomplete requests.
        //  flush() ;所有缓存记录被立刻发送
        JSONObject json=new JSONObject();
        JSONObject alert=new JSONObject();
        JSONObject message=new JSONObject();
        JSONObject enrich=new JSONObject();
        JSONArray array=new JSONArray();
        JSONObject marketing=new JSONObject();
        JSONObject finalResult=new JSONObject();

        finalResult.put("feedbackCode","861");
        finalResult.put("caseType","2");

        marketing.put("marketingClassify",0);
        marketing.put("feedbackCode","active");
        marketing.put("feedbackValue",0.01);
        array.add(marketing);
        message.put("CarNbr","qqqqqqqqqqqq");
        message.put("AcctNmbr","2012022920120229");
        message.put("TxnDte","20170101");
        message.put("TxnTme","142633");
        message.put("TxnAmt",100);
        message.put("CreditCurrCde","840");
        message.put("OrigMerName","测试");
        message.put("eventId","CashGift");
        enrich.put("CR_HANDPHONE__creditcardcustomer_gfyh","13576928253");
        json.put("effectiveFeedBack","");
        json.put("alert",alert);
        json.put("message",message);
        json.put("enrich",enrich);
        alert.put("finalResult",finalResult);
        for(int i = 0; i < 1; i++) {
            System.out.println(json.toJSONString());
            producer.send(new ProducerRecord("event-new",json.toJSONString()));
        }
        producer.close();

    }
}
