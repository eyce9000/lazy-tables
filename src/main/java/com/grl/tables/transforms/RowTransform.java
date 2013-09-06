package com.grl.tables.transforms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class RowTransform {
	public enum Mode {Union,Intersection};
	
	private Map<String,String> fields = new HashMap<String,String>();
	private Map<String,String> defaults = new HashMap<String,String>();
	private Map<String,ColumnTransform> colTransforms = new HashMap<String,ColumnTransform>();
	private Map<String,ColumnBuilder> colBuilders = new HashMap<String,ColumnBuilder>();
	private Mode mode = Mode.Union;
	public RowTransform(Map<String,String> fields){
		this.fields.putAll(fields);
	}
	public void setColumnTransforms(Map<String,ColumnTransform> colTransforms){
		this.colTransforms = new HashMap<String,ColumnTransform>(colTransforms);
	}
	public void setFieldDefaults(Map<String,String> defaults){
		this.defaults = new HashMap<String,String>(defaults);
	}
	public void setColumnBuilders(Map<String,ColumnBuilder> colBuilders){
		this.colBuilders = new HashMap<String,ColumnBuilder>(colBuilders);
	}
	public void setMode(Mode mode){
		this.mode = mode;
	}
	public Map<String,String> transform(Map<String,String> raw){
		HashSet<String> unProcessedKeys = new HashSet<String>(fields.values());
		unProcessedKeys.addAll(colBuilders.keySet());
		
		Map<String,String> processed = new HashMap<String,String>();
		
		for(Entry<String,String> rawEntry:raw.entrySet()){
			String key = rawEntry.getKey();
			String value = rawEntry.getValue();
			
			if(fields.containsKey(key)){
				String newKey = fields.get(key);
				processed.put(newKey,processValue(newKey,value,raw));
				unProcessedKeys.remove(newKey);
			}
			else if(mode==Mode.Union){
				processed.put(key,value);
			}
			else{
				//Do nothing
			}
		}
		if(mode==Mode.Union){
			for(String key:unProcessedKeys){
				String value = null;
				value = processValue(key,value,raw);
				processed.put(key,value);
			}
		}
		return processed;
	}
	
	private String processValue(String newKey,String value,Map<String,String> raw){
		if(value==null){
			if(colBuilders.containsKey(newKey)){
				value = colBuilders.get(newKey).buildColumn(raw);
			}
			else if(defaults.containsKey(newKey)){
				value = defaults.get(newKey);
			}
		}
		
		if(colTransforms.containsKey(newKey)){
			value = colTransforms.get(newKey).transform(value, newKey);
		}
		return value;
	}
}
