import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class HelloConsumer {

    private static String bootStrapServers = "localhost:9092";

    public static void main(String[] args){
        System.out.println("Hello World !!");

        String topic = "custom-kafka-topic";
        String groupId = "second_app";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        // Once consumer is created, it needs to subscribe to a topic (or) list of topics.
        consumer.subscribe(Arrays.asList(topic));

        //Once consumer is subscribed to topic, it needs to poll continously to print new Messages
        while(true){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(50));
            for(ConsumerRecord<String,String> record: consumerRecords){
                System.out.println("RECEIVED MESSAGE-> " + record.topic() + "---" +
                        " Key- " + record.key() +
                        " || Value-" + record.value());
                System.out.println("-------------------------------------------------------");
            }

        }
    }
}
