package com.grl.tables
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.xml.bind.annotation.XmlElement;


class SerializationTest2 {
	ObjectMapper mapper = new ObjectMapper()
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		TestObject obj1 = new TestObject()
		obj1.stringTest = "value1"
		obj1.intTest = 20
		obj1.enumTest = TestObject.TestEnum.VALUE2
		obj1.xmlElement = "elementValue"
		
		Serializer serializer = new RowObjectSerializer();
		
		assertEquals(["stringTest":"value1","intTest":"20","testEnum":"VALUE2","testXml":"elementValue"],serializer.serialize(obj1))
		
		
	}
	static class TestObject{
		public static enum TestEnum{VALUE1,VALUE2}
		String stringTest;
		int intTest;
		@JsonProperty("testEnum")
		TestEnum enumTest;
		@XmlElement(name="testXml")
		String xmlElement;
	}

}
