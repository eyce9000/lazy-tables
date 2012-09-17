package com.grl.tables;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.grl.tables.serializers.StringSerializer;

import au.com.bytecode.opencsv.CSVWriter;

public class Table implements Serializable,Iterable<Table.Row>{
	private Map<String,Integer> columnIndexes = new HashMap<String,Integer>();
	private List<String> columnTitles = new ArrayList<String>();
	private Map<String,ColumnSerializer> columnSerializers = new HashMap<String,ColumnSerializer>();
	private List<Row> data = new ArrayList<Row>();
	public Table(){
		
	}
	public Table(List<Map<String,Object>> values){
		for(Map<String,Object> row:values){
			appendRow(row);
		}
	}
	
	public void appendRow(Map<String,Object> row){
		for(String key:row.keySet()){
			if(!columnIndexes.containsKey(key)){
				columnIndexes.put(key,columnTitles.size());
				columnTitles.add(key);
			}
		}
		data.add(new Row(row));
	}
	
	public void appendAll(Table table){
		for(Row row:table.data){
			appendRow(row);
		}
	}
	
	public void setColumnIndex(int index,String columnTitle){
		if(columnIndexes.containsKey(columnTitle)){
			columnTitles.remove(columnIndexes.get(columnTitle).intValue());
		}
		columnTitles.add(index, columnTitle);
		
		recalculateColumnIndexes();
	}
	
	public void setFirstColumns(String...titles){
		for(int i=0; i<titles.length; i++){
			setColumnIndex(i,titles[i]);
		}
	}
	
	public void insertStaticColumn(int index,String columnTitle,Object value){
		columnTitles.add(index, columnTitle);
		for(Row row:data){
			row.put(columnTitle, value);
		}
		recalculateColumnIndexes();
	}
	
	private void recalculateColumnIndexes(){
		columnIndexes.clear();
		for(int i=0; i<columnTitles.size(); i++){
			columnIndexes.put(columnTitles.get(i), i);
		}
	}
	
	public List<String> getColumnsTitles(){
		return new ArrayList<String>(columnTitles);
	}
	
	public <T> List<T> getColumnValues(String columnTitle,Class<T> type){
		List<T> values = new ArrayList<T>();
		for(Row row:data){
			values.add(row.getAs(columnTitle, type));
		}
		return values;
	}
	public boolean hasColumn(String columnTitle){
		return this.columnIndexes.containsKey(columnTitle);
	}
	public boolean isColumnNumeric(String columnTitle){
		for(int i=0; i<data.size(); i++){
			Object value = data.get(i).get(columnTitle);
			if(value !=null)
				return (value instanceof Double ||
						value instanceof Integer ||
						value instanceof Float);
						
		}
		return false;
	}
	public Class<?> getColumnType(String columnTitle){
		for(int i=0; i<data.size(); i++){
			Object value = data.get(i).get(columnTitle);
			if(value!=null)
				return value.getClass();
		}
		return null;
			
	}
	public SummaryStatistics getColumnSummaryStatistics(String columnTitle){
		if(!isColumnNumeric(columnTitle))
			return null;

		SummaryStatistics stats = new SummaryStatistics();
		for(int i=0; i<data.size(); i++){
			Row row = data.get(i);
			Double value = row.getAs(columnTitle, Double.class);
			if(value==null){
				Integer intValue = row.getAs(columnTitle, Integer.class);
				if(intValue!=null)
					value = intValue.doubleValue();
			}
			if(value==null){
				Float floatValue = row.getAs(columnTitle, Float.class);
				if(floatValue!=null)
					value = floatValue.doubleValue();
			}
			if(value != null)
				stats.addValue(value.doubleValue());
		}
		return stats;
	}
	public double[] getColumnValuesDouble(String columnTitle){
		double[] values = new double[data.size()];
		for(int i=0; i<data.size(); i++){
			Row row = data.get(i);
			Double value = row.getAs(columnTitle, Double.class);
			if(value==null){
				Integer intValue = row.getAs(columnTitle, Integer.class);
				if(intValue!=null)
					value = intValue.doubleValue();
			}
			if(value==null){
				Float floatValue = row.getAs(columnTitle, Float.class);
				if(floatValue!=null)
					value = floatValue.doubleValue();
			}
			if(value != null)
				values[i] = value.doubleValue();
		}
		return values;
	}
	
	public int[] getColumnValuesInteger(String columnTitle){
		int[] values = new int[data.size()];
		for(int i=0; i<data.size(); i++){
			Row row = data.get(i);

			Integer value = row.getAs(columnTitle, Integer.class);
			if(value==null){
				Double dblValue = row.getAs(columnTitle, Double.class);
				if(dblValue!=null)
					value = dblValue.intValue();
			}
			if(value==null){
				Float floatValue = row.getAs(columnTitle, Float.class);
				if(floatValue!=null)
					value = floatValue.intValue();
			}
			if(value != null)
				values[i] = value.intValue();
		}
		return values;
	}
	public void flushRowsToCSV(CSVWriter writer) throws IOException{
		for(Row row:data){
			String[] rowData = new String[columnTitles.size()];
			for(String key:row.keySet()){
				ColumnSerializer serializer = columnSerializers.get(key);
				if(serializer == null) 
					serializer = new StringSerializer();
				rowData[columnIndexes.get(key)] = serializer.serialize(row.get(key));
			}
			writer.writeNext(rowData);
		}
		writer.flush();
		data.clear();
	}
	public CSVWriter startWriteToCSV(File out) throws IOException{
		CSVWriter writer = new CSVWriter(new FileWriter(out));
		writer.writeNext(columnTitles.toArray(new String[columnTitles.size()]));
		writer.flush();
		return writer;
	}
	public void writeToCSV(File out) throws IOException{
		CSVWriter writer = new CSVWriter(new FileWriter(out));
		writer.writeNext(columnTitles.toArray(new String[columnTitles.size()]));
		for(Row row:data){
			String[] rowData = new String[columnTitles.size()];
			for(String key:row.keySet()){
				ColumnSerializer serializer = columnSerializers.get(key);
				if(serializer == null) 
					serializer = new StringSerializer();
				rowData[columnIndexes.get(key)] = serializer.serialize(row.get(key));
			}
			writer.writeNext(rowData);
		}
		writer.flush();
		writer.close();
	}
	
	public void clearRows(){
		data = new ArrayList<Row>();
	}
	@Override
	public Iterator<Row> iterator() {
		return data.iterator();
	}
	
	
	public static class Row extends HashMap<String,Object>{
		public Row(){
			super();
		}
		public Row(Map<String,Object> data){
			super(data);
		}
		public <T> T getAs(String key,Class<T> type){
			Object value = get(key);
			try{
				return type.cast(value);
			}
			catch(ClassCastException ex){
				return null;
			}
		}
	}





	
}


