import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import akka.dispatch.Foreach;
import scala.Tuple2;



public class CookiesStreaming {

	private static HashMap<String, String> infovisit;
	private static HashMap<String, String> infopage;
	private static HashMap<String, String> infotemp;
	private static HashMap<String, Date> estat;
	private static HashMap<String, String> visits;
	
	static Date DateRef = null;
	static LocalDateTime TimerIni = LocalDateTime.now();
	static LocalDateTime Tnow = LocalDateTime.now();
	
	public static void main(String[] args) throws InterruptedException {
		
		SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaStreamingContext streamContext = new JavaStreamingContext(context, new Duration(1000));
        streamContext.checkpoint("checkpoint");
        context.setLogLevel("WARN");
        
        estat = Maps.newHashMap();
        infovisit = Maps.newHashMap();
        infotemp = Maps.newHashMap();
        infopage = Maps.newHashMap();
        visits = Maps.newHashMap();
        processCookies(streamContext);
        
        streamContext.start();
        streamContext.awaitTermination();
        
        
	}
	
	public static void processCookies(JavaStreamingContext streamContext) {
		final double VISIT_TIMEOUT = 2;				// minutes
		
		
		Map<String, Integer> topicMap = Maps.newHashMap();
		topicMap.put("testalexandria3", 1);
		
		JavaPairReceiverInputDStream<String, String> kafkaStream = 
			KafkaUtils.createStream(streamContext,"localhost:2181","xxx",topicMap);
		
		// Pairs <cookie, data>
		JavaPairDStream<String,Date> pairsCookieData = (JavaPairDStream<String, Date>) kafkaStream.mapToPair(s -> {
			
			System.out.println("pairsCookieData: ");
			String cookie = s._2.split("\\|")[6];
			String newdate = s._2.split("\\|")[3];
			String[] field = s._2.split("\\|");
				String Oid = field[0];
				String Url = field[1];
				String RefUrl = field[2];
				String InsertDate = field[3];
				String Atm_ID = field[6];    // cookie
			infopage.put(cookie, Oid + "," + Url + "," + RefUrl + "," + InsertDate + "," + Atm_ID);
			if (infotemp.get(cookie)==null)
				{
					infotemp.put(Oid, Url + "," + RefUrl + "," + InsertDate + "," + Atm_ID + ", 1"); 	// register new Oid and add ID_Visita
				}
			System.out.println("Cookie: "+cookie);
			System.out.println("newdate: "+newdate);
			return new Tuple2<String,String>(cookie, newdate);
		}).mapToPair(f -> {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSSSSS");
			System.out.println("DateRef1: "+DateRef);
			if (DateRef==null) { DateRef=dateFormat.parse(f._2.replace("\"", ""));}
			return new Tuple2<String,Date>(f._1,dateFormat.parse(f._2.replace("\"", "")));
		}).filter(x -> !x._1.isEmpty());
				
		pairsCookieData.foreachRDD(rdd -> {
			rdd.foreach(pairCookieData -> {
				String cookie = pairCookieData._1();
				Date newdate = pairCookieData._2();
				System.out.println("Cookie: " + cookie + " newdate: "+newdate);
				System.out.println("Pending: "+estat.size());
				if (estat.containsKey(cookie)) {
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSSSSS");
					Date dateStart = estat.get(cookie);
					Date dateStop = newdate;
					System.out.println("dateStart: "+dateStart);
					System.out.println("dateStop: "+dateStop);
					long diff = dateStop.getTime() - dateStart.getTime();
					long diffMinutes = diff / (60 * 1000) % 60;
					System.out.println("Diff: " + Long.toString(diffMinutes));
					
					if (diffMinutes>VISIT_TIMEOUT) {
						System.out.println("new visit! ----------------------------------------------------------------------------------------------------------------------");
						// visits.put(cookie, (int) diffMinutes);
						
						estat.remove(cookie);
						String[] field = infotemp.get(cookie).split("\\|");
						String Oid = field[0];
						String Url = field[1];
						String RefUrl = field[2];
						String InsertDate = field[3];
						String Atm_ID = field[6];    // cookie
					infopage.put(Oid, cookie /* Atm_ID */ + "," + Url + "," + RefUrl + "," + InsertDate + "," + Atm_ID);
						visits.put(Oid, cookie /* Atm_ID */ + "," + Url + "," + RefUrl + "," + InsertDate + "," + Atm_ID + Integer.toOctalString((int) diffMinutes));
						
						estat.remove(cookie);
						// jdbc.sql("insert...")
					}	
				}
				else 
				{
					estat.put(pairCookieData._1(),pairCookieData._2());
				}
					
					
			});
			System.out.println("Estat size:"+estat.size());
			System.out.println("Visits size: "+visits.size());

			
			Tnow = LocalDateTime.now();
			
			System.out.println("Tnow " + Tnow);
			System.out.println("TimerIni " + TimerIni);
			
			long diffMinT = ChronoUnit.MINUTES.between(TimerIni, Tnow);
			long diffMinS = ChronoUnit.SECONDS.between(TimerIni, Tnow);

			System.out.println("DiffMinT: " + diffMinT);
			System.out.println("DiffMinS: " + diffMinS);
			
			if (diffMinT >= VISIT_TIMEOUT) {				
				TimerIni = LocalDateTime.now();
				
				
				estat.forEach((cookie,v) -> 
				{					
					long diff =  DateRef.getTime() - v.getTime();
					long diffMinutes = diff / (60 * 1000) % 60;
					if (diffMinutes>=VISIT_TIMEOUT)
						{
							estat.remove(cookie);
							String[] field = infotemp.get(cookie).split("\\|");
							String Oid = field[0];
							String Url = field[1];
							String RefUrl = field[2];
							String InsertDate = field[3];
							String Atm_ID = field[6];    // cookie
						infopage.put(Oid, cookie /* Atm_ID */ + "," + Url + "," + RefUrl + "," + InsertDate + "," + Atm_ID);
							visits.put(Oid, cookie /* Atm_ID */ + "," + Url + "," + RefUrl + "," + InsertDate + "," + Atm_ID + Integer.toOctalString((int) diffMinutes));
							System.out.println("remove Estat: " +estat.size());
						}
					System.out.println("PURGE....................................................................");
					
					DateRef.setTime(DateRef.getTime()+(60000*2));  // 2 minunts purga
					//System.out.println("Diff: " + Long.toString(diffMinutes));
				//	long diff = .getTime() - dateref.getTime();
					System.out.println("infopageget: " + infopage.get(cookie).split(","));
				});
				
				try {
					Thread.sleep(4000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if (visits.size() % 50 == 0)
				{
				//System.out.println(visits.toString());	
				visits.forEach((k,v) ->
						{
						});
				}
					
    return null;
  });
	
// if exists update
		
	//	for (String s : estat.keySet()) {
			//if (estat.get(s) /*retorna data*/) {
				//comprovar si la data es > now()-30min
					//si, genero visita
					//elimino s del hashmap estat
		//	}
		//}
		
		pairsCookieData.print();

		//kafkaStream.print();
	}
}
