package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) {
        var producer = new KafkaProducer<String, String>(properties());
        String value = "1,233,123";
        var producerRecord = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
        try {
            producer.send(producerRecord, (data, err) -> {
                if (err != null) {
                    err.printStackTrace();
                }

                System.out.println("Enviando mensagem para o tópico: " + data.topic() + ":::" + data.partition()
                + "/" + data.offset() + ":::" + data.timestamp());
            }).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
