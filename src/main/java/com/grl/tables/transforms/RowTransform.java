package com.grl.tables.transforms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class RowTransform {
	public enum Mode {Union,Intersection};
	
	private Map<String,List<String>> newFieldsByOldFieldName = new HashMap<String,List<String>>();
	private Map<String,String> defaults = new HashMap<String,String>();
	private Map<String,ColumnTransform> transformsByNewFieldName = new HashMap<String,ColumnTransform>();
	private Map<String,ColumnBuilder> buildersByNewFieldName = new HashMap<String,ColumnBuilder>();
	private Mode mode = Mode.Union;
	RowTransform(){}
	void setFieldNameMapping(Map<String,List<String>> fields){
		this.newFieldsByOldFieldName.putAll(fields);
	}
	
	void setColumnTransforms(Map<String,ColumnTransform> colTransforms){
		this.transformsByNewFieldName.putAll(colTransforms);
	}
	void setFieldDefaults(Map<String,String> defaults){
		this.defaults.putAll(defaults);
	}
	void setColumnBuilders(Map<String,ColumnBuilder> colBuilders){
		this.buildersByNewFieldName.putAll(colBuilders);
	}
	void setMode(Mode mode){
		this.mode = mode;
	}
	
	public Map<String,String> transform(Map<String,String> raw){
		HashSet<String> unProcessedKeys = new HashSet<String>();
		for(List<String> fieldList:newFieldsByOldFieldName.values()){
			unProcessedKeys.addAll(fieldList);
		}
		unProcessedKeys.addAll(buildersByNewFieldName.keySet());
		
		Map<String,String> processed = new HashMap<String,String>();
		
		for(Entry<String,String> rawEntry:raw.entrySet()){
			String key = rawEntry.getKey();
			String value = rawEntry.getValue();
			
			if(newFieldsByOldFieldName.containsKey(key)){
				for(String newKey:newFieldsByOldFieldName.get(key)){
					processed.put(newKey,processValue(newKey,value,raw));
					unProcessedKeys.remove(newKey);
				}
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
			if(buildersByNewFieldName.containsKey(newKey)){
				value = buildersByNewFieldName.get(newKey).buildColumn(raw);
			}
			else if(defaults.containsKey(newKey)){
				value = defaults.get(newKey);
			}
		}
		
		if(transformsByNewFieldName.containsKey(newKey)){
			value = transformsByNewFieldName.get(newKey).transform(value, newKey);
		}
		return value;
	}
}
