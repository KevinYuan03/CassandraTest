package com.acxiom.test.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

public class CQLTest {

	private static void createSchema(String schemaName,Cluster cluster){
		Session session = cluster.connect();
		ResultSet rows = session.execute("SELECT * from system.schema_keyspaces where keyspace_name = '" + schemaName + "'");
		if(rows.isExhausted()){
			session.execute("CREATE KEYSPACE " + schemaName + " WITH replication " + 
				      "= {'class':'SimpleStrategy', 'replication_factor':3};");
		}
//		for(Row row:rows){
//			System.out.println(row.getString("keyspace_name"));
//		}
		session.close();
	}
	
	private static void createTable(String schemaName,Cluster cluster){
		Session session = cluster.connect(schemaName);
		session.execute("CREATE TABLE IF NOT EXISTS songs (" +
            "id uuid PRIMARY KEY," + 
            "title text," + 
            "album text," + 
            "artist text," + 
            "tags set<text>" + 
            ");");
		session.close();
	}
	
	private static void insertRow(String schemaName,Cluster cluster){
		Session session = cluster.connect(schemaName);
		session.execute("INSERT INTO songs (id, title, album, artist, tags) " +
      "VALUES (" +
      	  UUIDs.random().toString() + "," +
          "'La Petite Tonkinoise'," +
          "'Bye Bye Blackbird'," +
          "'Joséphine Baker'," +
          "{'jazz', '2013'})" +
          ";");
		session.close();
	}
	
	private static void listRows(String schemaName,Cluster cluster){
		Session session = cluster.connect(schemaName);
		ResultSet rows = session.execute("select * from songs");
		for(Row row:rows){
			System.out.println("id:" + row.getUUID("id"));
		}
		session.close();
	}
	
	public static void main(String[] args) {
		Cluster cluster = null;
		String schemaName = "test_keyuan";
		try{
			cluster = Client.getCluster("127.0.0.1");
			createSchema(schemaName, cluster);
			createTable(schemaName, cluster);
			insertRow(schemaName, cluster);
			listRows(schemaName, cluster);
			
			
		}finally{
			if(cluster != null){
				cluster.close();
			}
		}

		
	}

}
