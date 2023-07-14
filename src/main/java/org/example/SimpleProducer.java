package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger((SimpleProducer.class));
    // topic name : record 인스턴스 생성 시에 사용됨
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "192.168.0.101:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();          // producer 인스턴스 생성을 위한 옵션들(key, value). 필수 옵션, 선택 옵션 존재
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 직렬화 클래스 선언. String 객체 전송을 위해 StringSerializer 사용
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);      // config를 인자로 넘겨서 producer 생성

        String messageValue = "testMessage";
        // kafka broker로 보내기 위한 레코드를 생성. 키, 값의 타입은 직렬화 클래스에서와 동일하게 설정(String)
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        producer.send(record);              // 배치 전송(즉각적 전송X)
        logger.info("{}", record);
        producer.flush();                   // producer buffer의 레코드 배치를 브로커로 전송
        producer.close();
    }
}
