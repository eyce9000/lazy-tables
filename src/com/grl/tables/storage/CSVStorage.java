package com.grl.tables.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.grl.tables.ColumnSerializer;
import com.grl.tables.RowSerializer;
import com.grl.tables.Table;
import com.grl.tables.Table.Row;
import com.grl.tables.events.RowReadListener;

public class CSVStorage {
	public void storeTable(File file, Table table, boolean overwrite) throws IOException{
		RowSerializer serializer = new RowSerializer();
		if(file.getParentFile()!=null)
			file.getParentFile().mkdirs();
		CSVWriter writer = new CSVWriter(new FileWriter(file,!overwrite));
		if(overwrite) //Write Column Headers
			writer.writeNext(table.getColumnsTitles().toArray(new String[table.columnCount()]));
		for(Row row:table){
			String[] rowData = new String[table.columnCount()];
			for(String key:row.keySet()){
				rowData[table.getColumnIndices().get(key)] = row.get(key);
			}
			writer.writeNext(rowData);
		}
		writer.flush();
		writer.close();
	}
	
	public void readTable(File file, RowReadListener listener) throws Exception{
		CSVReader reader = new CSVReader(new FileReader(file));
		readTable(reader,listener);
	}
	public void readTable(File file, RowReadListener listener, char separator) throws Exception{
		CSVReader reader = new CSVReader(new FileReader(file),separator);
		readTable(reader,listener);
	}
	public void loadIntoTable(Table table, File file) throws Exception{
		CSVReader reader = new CSVReader(new FileReader(file));
		loadIntoTable(table,reader);
		
	}
	public void loadIntoTable(Table table, File file, char separator) throws Exception{
		CSVReader reader = new CSVReader(new FileReader(file),separator);
		loadIntoTable(table,reader);
	}
	public void loadIntoTable(final Table table,CSVReader reader) throws Exception{
		readTable(reader,new RowAppender(table));
	}
	public Table loadTable(Reader inputReader) throws Exception{
		CSVReader reader = new CSVReader(inputReader);
		return loadTable(reader);
	}
	public Table loadTable(Reader inputReader, char separator) throws Exception{
		CSVReader reader = new CSVReader(inputReader,separator);
		return loadTable(reader);
	}
	public Table loadTable(File file) throws Exception{
		CSVReader reader = new CSVReader(new FileReader(file));
		return loadTable(reader);
	}
	public Table loadTable(File file, char separator) throws Exception{
		CSVReader reader = new CSVReader(new FileReader(file),separator);
		return loadTable(reader);
	}
	private Table loadTable(CSVReader reader) throws Exception{
		Table table = new Table();
		RowAppender appender = new RowAppender(table);
		readTable(reader,appender);
		return table;
	}
	private void readTable(CSVReader reader, RowReadListener listener) throws Exception{
		String[] header = reader.readNext();
		String[] row;
		listener.readStart();
		while((row=reader.readNext())!=null){
			listener.onRowRead(readRow(row,header));
		}
		listener.readComplete();
	}
	
	public Row readRow(String[] rowData, String[] header){
		Row row = new Row();
		for(int i=0; i<header.length; i++){
			Object value;
			String key = header[i];
			String rawValue = null;
			if(i<rowData.length){
				rawValue = rowData[i];
			}
			if(rawValue == null || rawValue.length()==0){
				row.put(key, null);
				continue;
			}
			//Is the value numeric?
			//try{
			//	value = Double.parseDouble(rawValue);
			//	row.put(key, value);
			//	continue;
			//}
			//catch(NumberFormatException ex){
				
			//}
			row.put(key, rawValue);
		}
		return row;
	}
	
	class RowAppender implements RowReadListener{
		private Table table;
		public RowAppender(Table table){
			this.table = table;
		}
		@Override
		public void onRowRead(Row row) {
			table.appendRow(row);
		}
		@Override
		public void readStart() {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void readComplete() {
			// TODO Auto-generated method stub
			
		}
	}
}
