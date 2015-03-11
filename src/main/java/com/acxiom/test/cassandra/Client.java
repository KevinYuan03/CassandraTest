package com.acxiom.test.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class Client {
	//private Cluster cluster;

	   public static Cluster getCluster(String node) {
		   Cluster cluster = Cluster.builder().addContactPoint(node).build();
	      Metadata metadata = cluster.getMetadata();
	      System.out.printf("Connected to cluster: %s\n", 
	            metadata.getClusterName());
	      for ( Host host : metadata.getAllHosts() ) {
	         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
	               host.getDatacenter(), host.getAddress(), host.getRack());
	      }
	      
	      return cluster;
	   }

//	   public void close() {
//	      cluster.close();
//	   }

}
