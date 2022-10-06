package com.ing.demo.mvnglouddemo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;

@SpringBootApplication
public class MvnglouddemoApplication implements CommandLineRunner {

	public static void main(String[] args) {
		
		System.out.println("STARTING THE APPLICATION");
		SpringApplication.run(MvnglouddemoApplication.class, args);
		System.out.println("APPLICATION FINISHED");
	}
	 
	   
	    public void run(String... args) {
	    	System.out.println("******EXECUTING : command line runner *****");
	 
	        for (int i = 0; i < args.length; ++i) {
	        	System.out.println("args[{}]: {}"+i);
	        }
	        String datasetName = "MY_DATASET_NAME";
		    String location = "US";
		    createDataset(datasetName,location);
	    }
	    
	    public static String createDataset(String datasetName, String location) {
	    	System.out.println("Calling createDataset method....");
			String newDatasetName=null;
	    try {
	      // Initialize client that will be used to send requests. This client only needs to be created
	      // once, and can be reused for multiple requests.
	      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	      DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).setLocation(location).build();
	      Dataset newDataset = bigquery.create(datasetInfo);
	      newDatasetName = newDataset.getDatasetId().getDataset();
	      System.out.println(newDatasetName + " created successfully");
	      return newDatasetName;
	    } catch (BigQueryException e) {
	      System.out.println("Dataset was not created. \n" + e.toString());
	    }
	    System.out.println("End createDataset method....");
	    return newDatasetName;
	   
	  }

}
