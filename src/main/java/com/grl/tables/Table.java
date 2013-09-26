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
import com.grl.tables.filters.AcceptAllColumnFilter;
import com.grl.tables.filters.AcceptAllRowFilter;
import com.grl.tables.filters.ColumnFilter;
import com.grl.tables.filters.RowFilter;

import au.com.bytecode.opencsv.CSVWriter;

public class Table implements Serializable, Iterable<Table.Row> {
	private Map<String, Integer> columnIndices = new HashMap<String, Integer>();
	private List<String> columnTitles = new ArrayList<String>();
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

	public Table filter(ColumnFilter columnFilter){
		return this.filter(new AcceptAllRowFilter(),columnFilter);
	}
	
	public Table filter(RowFilter rowFilter){
		return this.filter(rowFilter,new AcceptAllColumnFilter());
	}
	
	public Table filter(RowFilter rowFilter, ColumnFilter columnFilter){
		Table table = new Table();
		for(Row row : this){
			if(rowFilter.acceptsRow(row)){
				table.appendRow(row.filter(columnFilter));
			}
		}
		return table;
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
	public List<String> getColumnValues(String columnTitle) {
		List<String> values = new ArrayList<String>();
		for (Row row : data) {
			values.add(row.get(columnTitle));
		}
		return values;
	}
	public List<String> getColumnValues(int columnNumber){
		String columnTitle = getColumnTitle(columnNumber);
		return getColumnValues(columnTitle);
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
	public Map<String, Integer> getColumnCategoricalCount(String... columnTitles) {
		HashMap<String, Integer> counts = new HashMap<String, Integer>();
		for (int i = 0; i < data.size(); i++) {
			Row row = data.get(i);
			String combinedKey="";
			for(String columnTitle:columnTitles){
				String rawValue = row.get(columnTitle);
				if (rawValue == null) 
					rawValue = "NULL";

				if(!combinedKey.isEmpty())
					combinedKey+=",";
				
				combinedKey+=rawValue;
			}
			if (!counts.containsKey(combinedKey)) {
				counts.put(combinedKey, 1);
			} else {
				counts.put(combinedKey, counts.get(combinedKey) + 1);
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
		
		public Row filter(ColumnFilter columnFilter){
			Row row = new Row();
			for(String key:this.keySet()){
				if(columnFilter.acceptsColumn(key)){
					row.put(key,this.get(key));
				}
			}
			return row;
		}
	}

}
