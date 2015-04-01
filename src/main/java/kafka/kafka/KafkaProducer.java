package kafka.kafka;
import java.util.Date;
import java.util.Properties;
//import java.util.Properties;
import java.util.Random;

import org.apache.kafka.common.requests.ProduceRequest;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KafkaProducer 
{
	public static void main( String[] args )
	{
		int events =3;
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092, broker2:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "kafka.kafka.SimplePartitioner");
		props.put("request.required.acks", "1");
		//props.put("retry.backoff.ms", "1000");
		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		Random rnd = new Random();

		for (long nEvents = 0; nEvents < events; nEvents++) {
			long runtime = new Date().getTime();
			String ip = "192.168.2." + rnd.nextInt(255);
			String msg = runtime + ",www.example.com," + ip;
			System.out.println(msg);
			KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", msg);

			producer.send(data);
			
		}
		producer.close();
	}
}
