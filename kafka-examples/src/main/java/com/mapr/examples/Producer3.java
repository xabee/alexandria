package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;



import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;




/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class Producer3 {
    public static void main(String[] args) throws IOException {
    	
    	Properties props = new Properties();
    	 props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
    	 props.put(ProducerConfig.ACKS_CONFIG, "1");
    	 // props.put("retries", 1);
    	 props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    	 props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    	 props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    	 props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    	 props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    	 // props.put("metadata.broker.list", "localhost:9092");
    	 // props.put("serializer.class", "kafka.serializer.StringEncoder");
    	 // props.put("partitioner.class", "example.producer.SimplePartitioner");
    	 // props.put("request.required.acks", "1");

    	 
    	 
    	 KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    	 /*
    	  * for(int i = 0; i < 100; i++)
    	 	{
    		 	producer.send(new ProducerRecord<String, String>("testalexandria", Integer.toString(i), Integer.toString(i)));
    		 	System.out.println("Sending...");
    	 	}
    	 producer.close();
  	*/
        // set up the producer
        KafkaProducer<String, String> producer2;
        try (InputStream props2 = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props2);
            producer = new KafkaProducer<>(properties);
        }
        
        String CSVfile="/home/xabee/alexandria/datasets/whole_navigation.csv";

    	FileInputStream fis = new FileInputStream(CSVfile);
        
      //Construct BufferedReader from InputStreamReader
    	BufferedReader br = new BufferedReader(new InputStreamReader(fis));
     
    	String line = null;
    	
    	line = br.readLine();
    	int i=0;
        for (i=1; i<100000; i++)
        {
    		System.out.println(Integer.toString(i) + "-->>" + line);
        	producer.send(new ProducerRecord<String, String>(
                    "testalexandria",
                     line));
 			try {
					TimeUnit.MILLISECONDS.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					System.out.println("Error!");
					e.printStackTrace();
				}
        }
    	
    	
    	while ((line = br.readLine()) != null) 
    		{
 	
	    		System.out.println(line);

	    			producer.send(new ProducerRecord<String, String>(
	                       "testalexandria",
	                        line));
	    			try {
						TimeUnit.MILLISECONDS.sleep(10);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						System.out.println("Error!");
						e.printStackTrace();
					}
				
    		

    			
    		}	
    	System.out.println("EOF...");
    	producer.close();
 

    }
}

