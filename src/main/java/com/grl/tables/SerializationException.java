package com.grl.tables;

public class SerializationException extends Exception {
	public SerializationException(Throwable throwable){
		super(throwable);
	}
	public SerializationException(String message){
		super(message);
	}
}
