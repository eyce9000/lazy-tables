package com.grl.tables.serializers;

import com.grl.tables.ColumnSerializer;

public class StringSerializer implements ColumnSerializer{

	@Override
	public String serialize(Object value) {
		if(value!=null)
			return value.toString();
		return null;
	}

	@Override
	public Object deserialize(String value) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
