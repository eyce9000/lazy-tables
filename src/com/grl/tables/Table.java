package com.grl.tables;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.grl.tables.annotations.TableColumn;

import au.com.bytecode.opencsv.CSVWriter;

public class Table implements Serializable, Iterable<Table.Row> {
	private Map<String, Integer> columnIndices = new HashMap<String, Integer>();
	private List<String> columnTitles = new ArrayList<String>();
	private Map<String, ColumnSerializer> columnSerializers = new HashMap<String, ColumnSerializer>();
	private List<Row> data = new ArrayList<Row>();

	public Table() {

	}

	public Table(List<Map<String, String>> values) {
		appendAll(values);
	}
	
	public Table(Table... tables){
		for(Table table:tables){
			this.appendAll(table);
		}
	}
	
	public int appendRow(Map<String, String> row) {
		for (String key : row.keySet()) {
			if (!columnIndices.containsKey(key)) {
				columnIndices.put(key, columnTitles.size());
				columnTitles.add(key);
			}
		}
		data.add(new Row(row));
		return data.size() - 1;
	}
	public void appendAll(Collection<Map<String,String>> rows){
		for (Map<String, String> row : rows) {
			appendRow(row);
		}
	}
	public void appendAll(Table table) {
		for (Row row : table.data) {
			appendRow(row);
		}
	}

	public void setColumnIndex(int index, String columnTitle) {
		if (columnIndices.containsKey(columnTitle)) {
			columnTitles.remove(columnIndices.get(columnTitle).intValue());
		}
		columnTitles.add(index, columnTitle);

		recalculateColumnIndexes();
	}

	public void setFirstColumns(String... titles) {
		for (int i = 0; i < titles.length; i++) {
			setColumnIndex(i, titles[i]);
		}
	}
	public void sortColumnsAlphabetical(){
		sortColumnsAlphabetical(true);
	}
	public void sortColumnsAlphabetical(boolean ascending) {
		Collections.sort(columnTitles);
		if(!ascending)
			Collections.reverse(columnTitles);
		recalculateColumnIndexes();
	}

	public void reverseColumnsOrder() {
		Collections.reverse(columnTitles);
		recalculateColumnIndexes();
	}

	public void insertStaticColumn(int index, String columnTitle, String value) {
		columnTitles.add(index, columnTitle);
		for (Row row : data) {
			row.put(columnTitle, value);
		}
		recalculateColumnIndexes();
	}

	private void recalculateColumnIndexes() {
		columnIndices.clear();
		for (int i = 0; i < columnTitles.size(); i++) {
			columnIndices.put(columnTitles.get(i), i);
		}
	}

	public List<String> getColumnsTitles() {
		return new ArrayList<String>(columnTitles);
	}

	public String getColumnTitle(int index){
		return columnTitles.get(index);
	}
	
	public Map<String, Integer> getColumnIndices() {
		return new HashMap<String, Integer>(this.columnIndices);
	}

	public int columnCount() {
		return columnTitles.size();
	}
	
	public int rowCount(){
		return data.size();
	}

	public Row getRow(int index) {
		return data.get(index);
	}

	public boolean hasColumn(String columnTitle) {
		return this.columnIndices.containsKey(columnTitle);
	}

	public boolean isColumnNumeric(String columnTitle) {
		for (int i = 0; i < data.size(); i++) {
			Object value = data.get(i).get(columnTitle);
			if (value != null)
				return (value instanceof Double || value instanceof Integer
						|| value instanceof Float || value instanceof Long);

		}
		return false;
	}

	public boolean isColumnString(String columnTitle) {
		for (int i = 0; i < data.size(); i++) {
			Object value = data.get(i).get(columnTitle);
			if (value != null)
				return (value instanceof String);

		}
		return false;
	}

	public Class<?> getColumnType(String columnTitle) {
		for (int i = 0; i < data.size(); i++) {
			Object value = data.get(i).get(columnTitle);
			if (value != null)
				return value.getClass();
		}
		return null;

	}

	public Map<String, Integer> getColumnCategoricalCount(String columnTitle) {
		HashMap<String, Integer> counts = new HashMap<String, Integer>();
		for (int i = 0; i < data.size(); i++) {
			Row row = data.get(i);
			Object rawValue = row.get(columnTitle);
			if (rawValue != null) {
				String value = rawValue.toString();

				if (!counts.containsKey(value)) {
					counts.put(value, 1);
				} else {
					counts.put(value, counts.get(value) + 1);
				}
			}
		}
		return counts;
	}


	public void clearRows() {
		data = new ArrayList<Row>();
	}

	@Override
	public Iterator<Row> iterator() {
		return data.iterator();
	}

	public List<Row> getRows() {
		return new ArrayList<Row>(data);
	}

	
	
	
	public static class Row extends HashMap<String, String> {
		public Row() {
			super();
		}

		public Row(Map<String, String> data) {
			super(data);
		}
//		
//		public Row(Object pojo){
//			super();
//			Field[] fields = pojo.getClass().getDeclaredFields();
//			for(Field field:fields){
//
//				boolean wasPublic = field.isAccessible();
//				field.setAccessible(true);
//				TableColumn annotation = field.getAnnotation(TableColumn.class);
//				if(annotation!=null){
//					String key = annotation.name();
//					try {
//						String value = (String)field.get(pojo);
//						if(key!=null){
//							this.put(key, value);
//						}
//					} catch(Exception ex){
//						ex.printStackTrace();
//					}
//				}
//				field.setAccessible(wasPublic);
//			}
//		}
//		
//		public <T> T deserialize(Class<T> type){
//			return deserialize(type,null);
//		}
//		
//		private <T> T deserialize(Class<T> type, String domain){
//			try{
//				T object = type.newInstance();
//				Field[] fields = object.getClass().getDeclaredFields();
//				
//				for(Field field :fields){
//					
//					boolean wasPublic = field.isAccessible();
//					field.setAccessible(true);
//					TableColumn annotation = field.getAnnotation(TableColumn.class);
//					if(annotation!=null){
//						String key = annotation.name();
//						try {
//							Object value = this.get(key);
//							if(key!=null && value!=null){
//								if(field.getType().isInstance("")){
//									field.set(object, value.toString());
//								}
//								else{
//									field.set(object,value);
//								}
//							}
//						} catch(Exception ex){
//							ex.printStackTrace();
//						}
//					}
//					field.setAccessible(wasPublic);
//				}
//				return object;
//			}
//			catch(Exception ex){
//				ex.printStackTrace();
//				return null;
//			}
//		}
		
//		public <T> T getAs(String key, Class<T> type) {
//			Object value = get(key);
//			try {
//				return type.cast(value);
//			} catch (ClassCastException ex) {
//				return null;
//			}
//		}
	}

}
