package com.grl.tables;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

class TableTest {
	Table table1
	@Before
	public void setUp() throws Exception {
		table1 = new Table([
			["key1":"k1value1","key2":"k2value1"],
			["key1":"k1value1","key2":"k2value2"],
			["key1":"k1value2"]
			])
	}
	@Test
	public void testCatagoricalCount(){
		assertEquals(
			["k1value1":2,"k1value2":1],
			table1.getColumnCategoricalCount("key1")
		)
		assertEquals(
			["k1value1,k2value1":1,"k1value1,k2value2":1,"k1value2,NULL":1],
			table1.getColumnCategoricalCount("key1","key2")
		)
	}

}
