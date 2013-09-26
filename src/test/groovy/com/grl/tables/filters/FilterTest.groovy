package com.grl.tables.filters;

import static org.junit.Assert.*;

import com.grl.tables.Table
import org.junit.Before;
import org.junit.Test;

class FilterTest {

	Table table1;
	@Before
	public void setUp() throws Exception {
		table1 = new Table([
			["key1":"value1","key2":"value1"],
			["key1":"value2","key2":"value2"],
			["key1":"value3","key2":null]
			
			])
	}

	@Test
	public void testColumnFilter() {
		Table filtered = table1.filter({String colname ->
			return colname=="key2"
		}as ColumnFilter)
		
		assertEquals(filtered.data,[["key2":"value1"],["key2":"value2"],["key2":null]])
		assertTrue(!(filtered.data.equals(table1.data)))
	}
	
	@Test
	public void testRowFilter(){
		Table filtered = table1.filter({Map<String,String> row ->
			return row["key2"]!=null
		}as RowFilter)
		
		assertEquals([
			["key1":"value1","key2":"value1"],
			["key1":"value2","key2":"value2"]],
		filtered.data)
	}

}
