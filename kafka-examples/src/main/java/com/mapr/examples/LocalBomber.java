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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;




/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class LocalBomber {
    public static void main(String[] args) throws IOException {
    	
    	Properties props = new Properties();
    	 // props.put("bootstrap.servers", "localhost:9092");
   	 	 props.put("bootstrap.servers", "localhost:9092");				
    	 props.put("acks", "all");
    	 props.put("retries", 0);
    	 props.put("batch.size", 16384);
    	 props.put("linger.ms", 1);
    	 props.put("buffer.memory", 33554432);
    	 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
         props.put("block.on.buffer.full", false);
    	 /// props.put("metadata.broker.list", "localhost:9092");
    	 /// props.put("serializer.class", "kafka.serializer.StringEncoder");
    	 //props.put("partitioner.class", "example.producer.SimplePartitioner");
    	 ///props.put("request.required.acks", "1");
         
         props.put("auto.commit.interval.ms", "true");
    	 
    	 
    	 KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String CSVfile="/home/xabee/alexandria/datasets/whole_navigation.csv";

    	FileInputStream fis = new FileInputStream(CSVfile);
        
      //Construct BufferedReader from InputStreamReader
    	BufferedReader br = new BufferedReader(new InputStreamReader(fis));
     
    	String line = null;
    	
    	line = br.readLine();
    	long i=0;
        /* for (i=1; i<100000; i++)
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
    	*/
    	
    	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    	
    	Date d1 = null;
    	Date d2 = null;
    	
    	line = br.readLine();
    	
    	
    	while ((line = br.readLine()) != null) 
    		{
	    		
	    while (!line.contains("2015-")) 
		    	{
	        		//System.out.println("not 2015!");
	    			br.skip(1000000000);
	    			line = br.readLine();
	    			System.out.println("==>>"+line);
		    	} 
	    
    		
    		System.out.println("Line " + Integer.toString((int) i) + " " + line);
	 	
	    		String[] part = line.split("\\|");
	    		
	    		try
	    			{
	    			System.out.println("Line " + Integer.toString((int) i) + " Time: " + part[3]);
	    			}
	    		catch (IndexOutOfBoundsException e)
	    		{
	    				// line 3710397
					e.printStackTrace();

	    		}
	    		/*
	    		if (i==0) 
	    		{
	    			try {
	    				formatter.format(part[3]);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    		}
	    		
	    		
	    	    try {
					d2 = format.parse(part[3]);
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	    		
	    	    
	    	    
	    	    long diff = d2.getTime() - d1.getTime();
	    	    long diffSeconds = diff / 1000;         
	    	    long diffMinutes = diff / (60 * 1000);         
	    	    long diffHours = diff / (60 * 60 * 1000);                      
	    	    System.out.println("Time in seconds: " + diffSeconds + " seconds.");         
	    	    System.out.println("Time in minutes: " + diffMinutes + " minutes.");         
	    	    System.out.println("Time in hours: " + diffHours + " hours.");
	    	    */
	    	    
	    		//if (line.contains("3333"))
				//{
	    		producer.send(new ProducerRecord<String, String>(
	                       "testalexandria3",
	                        line));
	    		System.out.println("Length: " + line.length() );
	    			try {
						TimeUnit.MILLISECONDS.sleep(1000);  // <---------------------
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						System.out.println("Error!");
						e.printStackTrace();
					}
	    			
				i++;
				
				if (i % 2000 == 0)
				{
					// restart producer
					producer.close();
					producer = null;
					try {
						TimeUnit.MILLISECONDS.sleep(1000);   // ---------------------------------------------- 
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			    	producer = new KafkaProducer<>(props);

					System.out.println("Restart producer!!!");
				}
    		}
    			
    		//}	
    	System.out.println("EOF...");
    	br.close();
    	producer.close();
 

    }
}
