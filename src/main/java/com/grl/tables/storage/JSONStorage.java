package com.grl.tables.storage;

import java.io.File;
import java.io.IOException;
import java.util.List;


import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.grl.tables.Table;
import com.grl.tables.Table.Row;

public class JSONStorage {
	
	
	public void storeTable(Table table, File file) throws JsonGenerationException, JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		storeTable(table,file,mapper);
	}
	public void storeTable(Table table, File file, ObjectMapper mapper) throws JsonGenerationException, JsonMappingException, IOException{
		TableContainer cont = new TableContainer();
		cont.rows = table.getRows();
		mapper.writeValue(file, cont);
	}
	
	public Table loadTable(File file) throws JsonParseException, JsonMappingException, IOException{
		Table table = new Table();
		loadIntoTable(file,table);
		return table;
	}
	
	public void loadIntoTable(File file, Table table)throws JsonParseException, JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		TableContainer cont = mapper.readValue(file, TableContainer.class);
		for(Row row:cont.rows){
			table.appendRow(row);
		}
		
	}
	
}

class TableContainer{
	List<Row> rows;

	public List<Row> getRows() {
		return rows;
	}

	public void setRows(List<Row> rows) {
		this.rows = rows;
	}
}

