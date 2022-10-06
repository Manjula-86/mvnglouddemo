package com.ing.demo.mvnglouddemo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

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
	        String datasetName = "CUSTOMER_US";
		    String location = "US";
		    
		    
		    datasetName = createDataset(datasetName,location);
		    String tableName = "CUSTOMER";
		    Schema schema =
		        Schema.of(
		            Field.of("customername", StandardSQLTypeName.STRING),
		            Field.of("customerid", StandardSQLTypeName.NUMERIC),
		            Field.of("isactive", StandardSQLTypeName.BOOL));
		    createTable(datasetName, tableName, schema);
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
	    
	    public static void createTable(String datasetName, String tableName, Schema schema) {
	    	System.out.println("Calling createTable method....");
	    	try {
	          // Initialize client that will be used to send requests. This client only needs to be created
	          // once, and can be reused for multiple requests.
	          BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

	          TableId tableId = TableId.of(datasetName, tableName);
	          TableDefinition tableDefinition = StandardTableDefinition.of(schema);
	          TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

	          bigquery.create(tableInfo);
	          System.out.println("Table created successfully");
	        } catch (BigQueryException e) {
	          System.out.println("Table was not created. \n" + e.toString());
	        }
	    	System.out.println("End createTable method....");
	      }

}
