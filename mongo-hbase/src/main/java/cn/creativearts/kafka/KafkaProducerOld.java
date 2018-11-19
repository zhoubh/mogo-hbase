package cn.creativearts.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author:zhoubh
 * @create:2018-11-10
 **/
public class KafkaProducerOld {
    private Producer<String, String> producer;

    public final static String TOPIC = "zbh-topic-test";

    private KafkaProducerOld() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "10.10.10.202:9092,10.10.10.203:9092,10.10.10.204:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    public void produce() {
        int messageNo = 0;
        final int COUNT = 10;

        while(messageNo < COUNT) {
            String data = String.format("hello kafka.javaapi.producer message %d", messageNo);

            KeyedMessage<String, String> msg = new KeyedMessage<String, String>(TOPIC, data);

            try {
                producer.send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }

            messageNo++;
        }

        producer.close();
    }

    public static void main(String[] args) {
        new KafkaProducerOld().produce();
    }


}
