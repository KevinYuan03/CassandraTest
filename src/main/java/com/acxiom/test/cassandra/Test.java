package com.acxiom.test.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Test {

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
	public static void main(String[] args) {
		Cluster cluster = null;
		String schemaName = "test_keyuan";
		try{
			cluster = Client.getCluster("127.0.0.1");
			createSchema(schemaName, cluster);
			
			
			
		}finally{
			if(cluster != null){
				cluster.close();
			}
		}

		
	}

}
