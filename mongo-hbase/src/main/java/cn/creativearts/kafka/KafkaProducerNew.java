package cn.creativearts.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * producer: org.apache.kafka.clients.producer.KafkaProducer;
 * */

public class KafkaProducerNew{

    private final KafkaProducer<String, String> producer;

   public final static String TOPIC = "zykj-moxie_phoneNo";
//    public final static String TOPIC = "test_" +
         //  "topic";

    private KafkaProducerNew() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.10.202:9092,10.10.10.203:9092,10.10.10.204:9092");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<String, String>(props);
    }

    public void produce() {
        String messageNo_s = "1860123458";
        int messageNo=0;
        final int COUNT = 200;

        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = String.format("%s%s",messageNo_s, key);

            try {
                producer.send(new ProducerRecord<String, String>(TOPIC, data));
            } catch (Exception e) {
                e.printStackTrace();
            }

            messageNo++;
        }

        producer.close();
    }

    public static void main(String[] args) {
        new KafkaProducerNew().produce();
    }


}