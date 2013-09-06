package com.grl.tables.serializers;

import com.grl.tables.ColumnSerializer;
import com.grl.tables.SerializationException;

public class PrimitiveSerializer extends ColumnSerializer{

	public PrimitiveSerializer(String format) {
		super(format);
	}

	@Override
	public String serialize(Object value) {
		if(getFormat().isEmpty())
			return value.toString();
		else
			return String.format(getFormat(),value);
	}

	@Override
	public Object deserialize(Class clazz,String value) throws SerializationException{
		if(clazz.isAssignableFrom(Integer.class) || clazz.isAssignableFrom(int.class)){
			return Integer.parseInt(value);
		}
		else if(clazz.isAssignableFrom(Long.class) || clazz.isAssignableFrom(long.class)){
			return Long.parseLong(value);
		}
		else if(clazz.isAssignableFrom(Double.class) || clazz.isAssignableFrom(double.class)){
			return Double.parseDouble(value);
		}
		else if(clazz.isAssignableFrom(Float.class) || clazz.isAssignableFrom(float.class)){
			return Float.parseFloat(value);
		}
		else if(clazz.isAssignableFrom(String.class)){
			return value;
		}
		else if(clazz.isEnum()){
			try{
				return Enum.valueOf( clazz,value);
			}
			catch(Exception ex){
				throw new SerializationException(ex);
			}
		}
		else{
			throw new SerializationException("Built in serializer unable to build instance of class "+clazz.getCanonicalName());
		}
	}


	@Override
	public boolean accepts(Class clazz) {
		if(clazz.isAssignableFrom(Integer.class) 
				|| clazz.isAssignableFrom(int.class)
				|| clazz.isAssignableFrom(Long.class) 
				|| clazz.isAssignableFrom(long.class)
				|| clazz.isAssignableFrom(Double.class)
				|| clazz.isAssignableFrom(double.class)
				|| clazz.isAssignableFrom(Float.class) 
				|| clazz.isAssignableFrom(float.class)
				|| clazz.isAssignableFrom(String.class)
				|| clazz.isEnum()){
			return true;
		}
		return false;
	}
	

}
