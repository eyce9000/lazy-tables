package com.grl.tables;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.grl.tables.annotations.TableColumn;

public class RowColumnSerializer implements Serializer{
	private Map<Class,Map<Field,ColumnSerializer>> configurations = new HashMap<Class,Map<Field,ColumnSerializer>>();
	
	public <T> T deserialize(Class<? extends T> clazz, Map<String,String> row){
		try{
			Map<Field,ColumnSerializer> serializers = getConfiguration(clazz);
			Constructor c = clazz.getDeclaredConstructor();
			c.setAccessible(true);
			T object = (T)c.newInstance();
			for(Entry<Field,ColumnSerializer> entry:serializers.entrySet()){
				Field field = entry.getKey();
				boolean wasPublic = field.isAccessible();
				field.setAccessible(true);
				TableColumn annotation = field.getAnnotation(TableColumn.class);
				if(annotation!=null){
					String key = annotation.name();
					try {
						String value = row.get(key);
						if(key!=null && value!=null){
							ColumnSerializer colSerializer = entry.getValue();
							if(colSerializer.accepts(field.getType())){
								try{
									Object valueTyped = entry.getValue().deserialize(field.getType(), value);
									field.set(object,valueTyped);
								}
								catch(IllegalAccessException iae){
									throw new Exception(String.format("Unable to set field at %s.%s",clazz.getName(),field.getName())
											,iae);
								}
							}
							else
								throw new SerializationException(String.format("Column serializer %s cannot deserialize the class %s",colSerializer.getClass().getName(),field.getType().getName()));
						}
					} catch(Exception ex){
						ex.printStackTrace();
					}
				}
			}
			return object;
		}
		catch(Exception ex){
			throw new RuntimeException(ex);
		}
	}
	public Map<String,String> serialize(Object pojo){
		Map<Field,ColumnSerializer> serializers = getConfiguration(pojo.getClass());
		Table.Row row = new Table.Row();
		for(Entry<Field,ColumnSerializer> entry:serializers.entrySet()){
			Field field = entry.getKey();
			boolean wasPublic = field.isAccessible();
			field.setAccessible(true);
			TableColumn annotation = field.getAnnotation(TableColumn.class);
			if(annotation!=null){
				String key = annotation.name();
				try {
					Object rawValue = field.get(pojo);
					if(rawValue!=null){
						String value = entry.getValue().serialize(rawValue);
						if(key!=null){
							row.put(key, value);
						}
					}
				} catch(Exception ex){
					ex.printStackTrace();
				}
			}
			field.setAccessible(wasPublic);
		}
		return row;
	}
	
	private Map<Field,ColumnSerializer> getConfiguration(Class clazz){
		if(!configurations.containsKey(clazz))
			configurations.put(clazz, configure(clazz));
		return configurations.get(clazz);
	}
	
	private Map<Field,ColumnSerializer> configure(Class clazz){
		Map<Field,ColumnSerializer> serializers = new HashMap<Field,ColumnSerializer>();
		try{
			for(Field field:clazz.getDeclaredFields()){
				TableColumn coldef = field.getAnnotation(TableColumn.class);
				if(coldef!=null){
					ColumnSerializer serializer = coldef.serializer().getDeclaredConstructor(String.class).newInstance(coldef.format());
					serializers.put(field,serializer);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return serializers;
	}
}
