package com.hitachi.cctransaction.dataflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitachi.cctransaction.dataflow.IKafkaConstants;
import com.hitachi.cctransaction.dataflow.ProducerCreator;
public class App {
    public static void main(String[] args) {
     
      final File folder = new File(args[0]);
      runProducer(folder);
     // listFilesForFolder(folder);
    }
    
    public static ArrayList<File> listFilesForFolder(File folder) {
    	ArrayList<File> listofFiles=new ArrayList<File>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
            	listofFiles.add(fileEntry);
            }
        }
        return listofFiles;
    }

	/*
	 * static void runConsumer() { Consumer<Long, String> consumer =
	 * ConsumerCreator.createConsumer(); int noMessageFound = 0; while (true) {
	 * ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000); // 1000
	 * is the time in milliseconds consumer will wait if no record is found at
	 * broker. if (consumerRecords.count() == 0) { noMessageFound++; if
	 * (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) // If no
	 * message found count is reached to threshold exit loop. break; else continue;
	 * }
	 */
          //print each record. 
	/*
	 * consumerRecords.forEach(record -> { System.out.println("Record Key " +
	 * record.key()); System.out.println("Record value " + record.value());
	 * System.out.println("Record partition " + record.partition());
	 * System.out.println("Record offset " + record.offset()); });
	
          // commits the offset of record to broker. 
           consumer.commitAsync();
        }
    consumer.close();
    }
     */
    static void runProducer(File folderPath) {
    	ObjectMapper mapper = new ObjectMapper();
    //	final File folder = new File("D:\\creditcard\\creditcardfraud");
        ArrayList<File> listOfFiles=listFilesForFolder(folderPath);
Producer<Long, String> producer = ProducerCreator.createProducer();

        for (int index = 0; index < listOfFiles.size(); index++) {
        	
        	File iFile=listOfFiles.get(index);
        	
        	List<TransactionData> transactionList= getTransactionJson(iFile);
        	
        	
        	for(TransactionData data: transactionList) {
        		String jsonTrasaction=null;
        		try {
				 jsonTrasaction=mapper.writeValueAsString(data);
				} catch (JsonProcessingException e1) {
					
					e1.printStackTrace();
				}
        	
        	
        	
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
            		jsonTrasaction + index);
            
            try {
            RecordMetadata metadata = producer.send(record).get();
                        System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
                 } 
            catch (ExecutionException e) {
                     System.out.println("Error in sending record");
                     System.out.println(e);
                  } 
             catch (InterruptedException e) {
                      System.out.println("Error in sending record");
                      System.out.println(e);
                  }
            
        	}
         }
    }
    
  public static List<TransactionData> getTransactionJson(File csvFile) {
	  List<TransactionData> transactions=null;
	  Pattern pattern = Pattern.compile(",");
	  try (BufferedReader in = new BufferedReader(new FileReader(csvFile));){
	       transactions = in.lines().skip(1)
	          .map(line -> {
	                  String[] x = pattern.split(line);
	                  return new TransactionData(x[0],
	                		  Float.parseFloat(x[1]),
	                		  Float.parseFloat(x[2]),
	                		  Float.parseFloat(x[3]),
	                		  Float.parseFloat(x[4]),
	                		  Float.parseFloat(x[5]),
	                		  Float.parseFloat(x[6]),
	                		  Float.parseFloat(x[7]),
	                		  Float.parseFloat(x[8]),
	                		  Float.parseFloat(x[9]),
	                		  Float.parseFloat(x[10]),
	                		  Float.parseFloat(x[11]),
	                		  Float.parseFloat(x[12]),
	                		  Float.parseFloat(x[13]),
	                		  Float.parseFloat(x[14]),
	                		  Float.parseFloat(x[15]),
	                		  Float.parseFloat(x[16]),
	                		  Float.parseFloat(x[17]),
	                		  Float.parseFloat(x[18]),
	                		  Float.parseFloat(x[19]),
	                		  Float.parseFloat(x[20]),
	                		  Float.parseFloat(x[21]),
	                		  Float.parseFloat(x[22]),
	                		  Float.parseFloat(x[23]),
	                		  Float.parseFloat(x[24]),
	                		  Float.parseFloat(x[25]),
	                		  Float.parseFloat(x[26]),
	                		  Float.parseFloat(x[27]),
	                		  Float.parseFloat(x[28]),
	                		  Float.parseFloat(x[29]),
	                		 x[30]);
	              })
	          .collect(Collectors.toList());
	      
	     
	      
  } catch (FileNotFoundException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
    
	  return transactions;
  }
  
} 