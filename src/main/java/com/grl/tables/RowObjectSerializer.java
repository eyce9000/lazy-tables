package com.grl.tables;
/* 
Copyright (c) 2014 George Lucchese

License: MIT License (http://opensource.org/licenses/MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;

public class RowObjectSerializer implements Serializer{
	TypeReference<HashMap<String,String>> def = new TypeReference<HashMap<String,String>>(){};
	ObjectMapper mapper;
	public RowObjectSerializer(){
		mapper = new ObjectMapper();
		mapper.setAnnotationIntrospector(new AnnotationIntrospectorPair(
				new JacksonAnnotationIntrospector(),
				new JaxbAnnotationIntrospector(mapper.getTypeFactory())
		));
	}
	public RowObjectSerializer(ObjectMapper mapper){
		this.mapper = mapper;
	}
	public ObjectMapper getMapper(){
		return mapper;
	}
	public <T> T deserialize(Class<? extends T> clazz, Map<String,String> row){
		return (T)mapper.convertValue(row, clazz);
	}
	
	public Map<String,String> serialize(Object obj){
		return (Map<String,String>)mapper.convertValue(obj,  def);
	}
}
