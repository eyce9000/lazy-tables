package com.grl.tables.transforms

import static org.junit.Assert.*
import org.junit.*

class RowTransformTest {
	Map<String,String> raw;
	@Before
	public void setup(){
		raw = ["key1":"value1","key2":"value2","key3":"value3"];
	}
	
	@Test
	public void testUnion(){
		RowTransformFactory builder = new RowTransformFactory()
		builder.putField("key1", "new-key1")
				.putFieldAndDefault("key4", "new-key4", "default-value4")
				.putFieldAndTransform("key2", "new-key2", {String value,String key ->
					return "transformed-"+value;
				} as ColumnTransform)
				.putFieldBuilder("new-key5", {Map<String,String> raw ->
					return raw.get("key1")+"-"+raw.get("key2");
				} as ColumnBuilder)
		RowTransform transform = builder.buildUnion()
		
		Map<String,String> processed = transform.transform(raw)
		
		Map<String,String> correctResult = [
			"new-key1":"value1",
			"new-key2":"transformed-value2",
			"key3":"value3",
			"new-key4":"default-value4",
			"new-key5":"value1-value2"
			]
		
		assertEquals(correctResult,processed)
	}
	
	@Test
	public void testIntersection(){
		RowTransformFactory builder = new RowTransformFactory()
		builder.putField("key1","new-key1")
		.putFieldAndTransform("key3", "new-key3", {String value,String key->
			return "transformed-"+value
			}as ColumnTransform)
		.putFieldBuilder("new-key5", {Map<String,String> raw ->
			return raw.get("key1")+"-"+raw.get("key2");
		} as ColumnBuilder)
		RowTransform transform = builder.buildIntersection()
		
		Map<String,String> processed = transform.transform(raw)
		
		Map<String,String> correctResult = [
			"new-key1":"value1",
			"new-key3":"transformed-value3"
			]
		
		assertEquals(correctResult,processed)
		
	}
}
