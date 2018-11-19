package cn.creativearts.kafka;


import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * @author:zhoubh
 * @create:2018-11-10
 **/
public class KafkaConsumerOld {
    private final ConsumerConnector consumerConnector;

    public final static String TOPIC = "zbh-topic-test";

    private KafkaConsumerOld() {
        Properties props = new Properties();
        props.put("zookeeper.connect","10.10.10.202:2181,10.10.10.203:2181,10.10.10.201:2181/kafka");
        props.put("group.id", "group-test-1");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringDecoder");

        ConsumerConfig config = new ConsumerConfig(props);

        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    public void consume() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KafkaProducerNew.TOPIC, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);

        List<KafkaStream<String, String>> streams = consumerMap.get(TOPIC);
        for (final KafkaStream stream : streams) {
            ConsumerIterator<String, String> it = stream.iterator();
            while (it.hasNext()) {
                System.out.println("this is kafka consumer : " + new String( it.next().message().toString()) );
            }
        }
    }

    public static void main(String[] args) {
        new KafkaConsumerOld().consume();
    }

}
