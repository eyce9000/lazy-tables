package com.grl.tables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyedTable<T> extends Table{
	private HashMap<String, HashMap<T,Integer>> keyIndices = new HashMap<String, HashMap<T, Integer>>();
	private List<String> keyedColumns = new ArrayList<String>();
	
	public KeyedTable(String...keys){
		super();
		setKeyColumns(keys);
	}
	
	public void setKeyColumns(String... keys){
		for(String key:keys){
			keyIndices.put(key, new HashMap<T,Integer>());
			this.keyedColumns.add(key);
		}
	}
	public int appendRow(Map<String,String> row){
		int index = super.appendRow(row);
		for(String key:keyedColumns){
			if(row.containsKey(key))
				keyIndices.get(key).put((T)row.get(key), index);
		}
		return index;
	}
	public Row getRow(String value){
		if(keyedColumns.size()==1){
			HashMap<T,Integer> columnKeys = keyIndices.get(keyedColumns.get(0));
			if(columnKeys.containsKey(value))
				return getRow(columnKeys.get(value));
		}
		return null;
	}
	public List<String> getKeyedColumns(){
		return new ArrayList<String>(keyedColumns);
	}
	public Row getRow(String column, String value){
		if(keyIndices.containsKey(column)){
			HashMap<T,Integer> columnKeys = keyIndices.get(column);
			if(columnKeys.containsKey(value))
				return getRow(columnKeys.get(value));
		}
		return null;
	}
}
