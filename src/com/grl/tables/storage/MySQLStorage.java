package com.grl.tables.storage;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.grl.tables.Table;

public class MySQLStorage {
	private Logger logger = LoggerFactory.getLogger(MySQLStorage.class);
	private Connection conn;

	public MySQLStorage(Connection conn) {
		this.conn = conn;
	}

	public Table tableFromResultSet(ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		Table table = new Table();
		while (rs.next()) {
			Table.Row row = new Table.Row();
			for (int i = 1; i <= md.getColumnCount(); i++) {
				String name = md.getColumnName(i);
				Object value = null;
				switch (md.getColumnType(i)) {

				case java.sql.Types.NUMERIC:
				case java.sql.Types.DOUBLE:
				case java.sql.Types.FLOAT:
					value = rs.getDouble(i);
					break;

				case java.sql.Types.INTEGER:
				case java.sql.Types.SMALLINT:
					value = rs.getInt(i);
					break;
				
				case java.sql.Types.BOOLEAN:
				case java.sql.Types.TINYINT:
					value = rs.getBoolean(i);
					break;
				
				default:
					value = rs.getString(i);
				}
				row.put(name, value);
			}
			table.appendRow(row);
		}
		return table;
	}
	
	public Table loadTable(String tableSchema,String tableName) throws SQLException{
		return tableFromResultSet(conn.createStatement().executeQuery("SELECT * FROM "+tableSchema+"."+tableName));
	}

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
	public void dropTable(String schemaName, String tableName) throws SQLException{
		String definition = "DROP TABLE "+schemaName+"."+tableName;
		Statement st = conn.createStatement();
		st.executeUpdate(definition);
		st.close();
	}
	public boolean storeTable(Table table, String schemaname, String tablename, boolean force) throws SQLException {
		if(tableExists(schemaname,tablename)){
			if(force){
				dropTable(schemaname,tablename);
				createTable(table,schemaname,tablename);
			}
		}
		else{
			createTable(table,schemaname,tablename);
		}
		
		String part1 = "INSERT INTO "+schemaname+"."+tablename+" (";
		String part2 = "VALUES(";
		for(int i=0; i<table.getColumnsTitles().size(); i++){
			String column = table.getColumnsTitles().get(i);
			String fixedColumn = column.replaceAll("\\s+", "_");
			part1+=" "+fixedColumn;
			part2+=" ?";
			if(i<table.getColumnsTitles().size()-1){
				part1+=",";
				part2+=",";
			}
		}
		part1+=") ";
		part2+=") ";
		String statement = part1+part2;
		logger.info(statement);
		PreparedStatement ps = conn.prepareStatement(statement);
		for(Table.Row row:table){
			for(int i=1; i<=table.getColumnsTitles().size(); i++){
				String column = table.getColumnsTitles().get(i-1);
				Object value = row.get(column);
				if(value instanceof Double){
					Double dblVal = (Double)value;
					if(!dblVal.isNaN() && !dblVal.isInfinite())
						ps.setDouble(i, dblVal.doubleValue());
				}
				else if(value instanceof Float)
					ps.setDouble(i, ((Float) value).doubleValue());
				else if(value instanceof Integer)
					ps.setInt(i, ((Integer) value).intValue());
				else if(value instanceof Boolean)
					ps.setBoolean(i, ((Boolean) value).booleanValue());
				else if(value instanceof double[])
					ps.setString(i, serializeArray((double[])value));
				else if(value instanceof int[])
					ps.setString(i, serializeArray((int[])value));
				else
					ps.setString(i, value.toString());
			}
			ps.executeUpdate();
		}
		return true;
	}

	private static String serializeArray(int[] data){
		String serialize = "[";
		for(int i=0; i<data.length; i++){
			serialize += data[i];
			if(i<data.length-1)
				serialize += ",";
		}
		serialize += "]";
		return serialize;
	}
	private static String serializeArray(double[] data){
		String serialize = "[";
		for(int i=0; i<data.length; i++){
			serialize += data[i];
			if(i<data.length-1)
				serialize += ",";
		}
		serialize += "]";
		return serialize;
	}
}
