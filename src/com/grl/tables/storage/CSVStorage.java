package com.grl.tables.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.grl.tables.ColumnSerializer;
import com.grl.tables.Table;
import com.grl.tables.Table.Row;
import com.grl.tables.serializers.StringSerializer;

public class CSVStorage {
	public void storeTable(File file, Table table, boolean overwrite) throws IOException{
		if(file.getParentFile()!=null)
			file.getParentFile().mkdirs();
		CSVWriter writer = new CSVWriter(new FileWriter(file,!overwrite));
		if(overwrite) //Write Column Headers
			writer.writeNext(table.getColumnsTitles().toArray(new String[table.columnCount()]));
		for(Row row:table){
			String[] rowData = new String[table.columnCount()];
			for(String key:row.keySet()){
				ColumnSerializer serializer = new StringSerializer();
				rowData[table.getColumnIndices().get(key)] = serializer.serialize(row.get(key));
			}
			writer.writeNext(rowData);
		}
		writer.flush();
		writer.close();
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
		String[] header = reader.readNext();
		String[] row;
		while((row=reader.readNext())!=null){
			table.appendRow(readRow(row,header));
		}
		return table;
	}
	
	public Row readRow(String[] rowData, String[] header){
		Row row = new Row();
		for(int i=0; i<header.length; i++){
			Object value;
			String key = header[i];
			String rawValue = rowData[i];
			if(rawValue.length()==0){
				row.put(key, null);
				continue;
			}
			//Is the value numeric?
			try{
				value = Double.parseDouble(rawValue);
				row.put(key, value);
				continue;
			}
			catch(NumberFormatException ex){
				
			}
			row.put(key, rawValue);
		}
		return row;
	}
}
