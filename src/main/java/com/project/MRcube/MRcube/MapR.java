package com.project.MRcube.MRcube;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class MapR {
	private static String connectionUrl;
	private static String jdbcDriverName;
	

      private static void loadConfiguration() throws IOException {
    	  connectionUrl = "jdbc:hive2://quickstart.cloudera:21050/;auth=noSasl";
          jdbcDriverName ="org.apache.hive.jdbc.HiveDriver";
      }
     
      static String executeQuery(String sqlStatement) {
  		System.out.println("Running Query: " + sqlStatement);

  		Connection con = null;

  		try {
  			loadConfiguration();
  			Class.forName(jdbcDriverName);
  			con = DriverManager.getConnection(connectionUrl);
  			Statement stmt = con.createStatement();
  			ResultSet rs = stmt.executeQuery(sqlStatement);
  			
  			JSONObject serverObj = new JSONObject();
  			
  			JSONArray jsonArray = new JSONArray();
  			try {
				while (rs.next()) {
					JSONObject rs1 = new JSONObject();
					
					ResultSetMetaData metaData = rs.getMetaData();
					int count = metaData.getColumnCount(); //number of column
					
					for (int i = 1; i <= count; i++)
					{
						rs1.put(metaData.getColumnName(i), rs.getString(i));
					}
					
  	  				jsonArray.add(rs1);
				}
  	  			
  	  		} catch (SQLException e) {
  	  			// TODO Auto-generated catch block
  	  			e.printStackTrace();
  	  		}
  			
  			serverObj.put("result", jsonArray);
  			return serverObj.toString();
  			
  		} catch (Exception e) {
  			System.out.println("Warning: table is already present");
  		} finally {
  			try {
  				con.close();
  			} catch (Exception e) {
  				// swallow
  			}
  		}
  		return null;
  	}
      
}