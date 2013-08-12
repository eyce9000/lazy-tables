package com.grl.tables;

public abstract class ColumnSerializer {
	protected final String format;
	public ColumnSerializer(String format){
		this.format = format;
	}
	public String getFormat() {
		return format;
	}
	public abstract String serialize(Object value) throws SerializationException;
	public abstract <T> T deserialize(Class<? extends T> clazz, String value) throws SerializationException;
	public abstract boolean accepts(Class clazz);
}
