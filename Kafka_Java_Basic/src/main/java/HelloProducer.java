import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HelloProducer {

    public static void main(String args[]){

        //Setting the Logger to this particular class
        Logger logger = LoggerFactory.getLogger(HelloProducer.class);

        String bootstrapServers = "localhost:9092";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try{
            //Creates a producer Kafka Class
            KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(props);

            // Details about the Topic and the messages
            String topic = "custom-kafka-topic";
            for(int i =0; i< 10 ; i++){
                String key = "id_" + String.valueOf(i);
                String value = "Dummy Kafka to Key- " + String.valueOf(i);

                // Creates a producer record to send to Kafka
                ProducerRecord<String,String> record = new ProducerRecord<>(topic,key,value);

                //Sending the record to the kafka, a callback fn is used to process the onCompletion Event
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metaData, Exception e) {
                        if(e==null){
                            System.out.println("Finished Sending the records to Kafka-" +
                                    " topic: " + metaData.topic() + "\n" +
                                    " partition: " +metaData.partition() + "\n" +
                                    " OffSet: " + metaData.offset() + "\n" +
                                    " TimeStamp: " + metaData.timestamp() + "\n" +
                                    " serializedKeySize: " + metaData.serializedKeySize() + "\n" +
                                    " serializedValueSize: " + metaData.serializedValueSize() + "\n");
                        }
                        else{
                            System.out.println ( "ERROR-ERROR-ERROR !!!" + e.getMessage());
                        }
                    }
                }).get(); //Get method is used to forceFully send it synchronously;
            }

            //This is required to force all data to be produced
            kafkaProducer.flush();
            // Stops the producer
            kafkaProducer.close();

            logger.info("Message sent from producer ");
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }

    }
}
