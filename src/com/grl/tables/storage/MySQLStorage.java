package com.grl.tables.storage;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.grl.tables.Table;

public class MySQLStorage extends SQLStorage {

	public MySQLStorage(Connection conn) {
		super(conn);
	}
	
	@Override
	public boolean tableExists(String tableSchema,String tableName)
			throws SQLException {

		DatabaseMetaData md = conn.getMetaData();
		Statement st = conn.createStatement();
		PreparedStatement ps = conn.prepareStatement("select * from information_schema.tables where " +
				"table_schema=? and table_name=?");
		ps.setString(1, tableSchema);
		ps.setString(2, tableName);
		ResultSet rs = ps.executeQuery();
		try {
			if (rs.next())
				return true;
			return false;
		} finally {
			rs.close();
		}

	}
	
	@Override
	public void createTable(Table table, String schemaName, String tableName) throws SQLException{
		String definition = "CREATE TABLE "+schemaName+"."+tableName+"(";
		for(int i=0; i<table.getColumnsTitles().size(); i++){
			String column = table.getColumnsTitles().get(i);
			String fixedColumn = column.replaceAll("\\s+", "_");
			definition+=" "+fixedColumn;
			if(table.isColumnNumeric(column)){
				definition+=" DOUBLE";
			}
			else{
				Class<?> type = table.getColumnType(column);
				if(type==null)
					logger.error("Completely null column:"+column);
				if(type.isAssignableFrom(Boolean.class))
					definition+=" TINYINT(1)";
				else if(type.isAssignableFrom(double[].class))
					definition+=" TEXT";
				else if(type.isAssignableFrom(int[].class))
					definition+=" TEXT";
				else
					definition+=" VARCHAR(512)";
			}
			if(i<table.getColumnsTitles().size()-1)
				definition+=",";
		}
		definition += ")";
		
		Statement st = conn.createStatement();
		st.executeUpdate(definition);
		st.close();
		
	}
}
