package stream.kafka;

import com.alibaba.fastjson.JSON;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import pojo.ArticlePojo;

import java.util.Date;
import java.util.Properties;

/**    kafka生产者
 * Created by Corn on 2017/4/5.
 */
public class kafkaProducer extends Thread{
    private String topic;

    public kafkaProducer(String topic){
        super();
        this.topic = topic;
    }


    @Override
    public void run() {
        Producer producer = createProducer();
        int i=0;
        ArticlePojo art=new ArticlePojo("我是测试",new Date().toString(),"我有一头小毛驴，从来也不骑","","");
        String a1= JSON.toJSON(art).toString();
        producer.send(new KeyedMessage<Integer, String>(topic, a1 ));
//        while(true){
//
//
//            try {
//                TimeUnit.SECONDS.sleep(100);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }

    private Producer createProducer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "localhost:2181");//声明zk
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "localhost:9092");// 声明kafka broker
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }


    public static void main(String[] args) {
        new kafkaProducer("sym_graduate").start();// 使用kafka集群中创建好的主题 test

    }
}
