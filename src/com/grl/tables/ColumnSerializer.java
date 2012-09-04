package com.grl.tables;

public interface ColumnSerializer<T> {
	public String serialize(T value);
	public T deserialize(String value);
}
