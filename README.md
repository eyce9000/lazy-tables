lazy-tables
===========

A java library for manipulating and storing tables (lists of hash maps).


Usage Example
------------
```java
Table table = new Table();

//Create data row
Map<String,String> dataRow = new HashMap<String,String>();
dataRow.put("Field1","Value1");
dataRow.put("Field2","Value2");

//Add data row to table
table.appendRow(dataRow);

//Dump table to CSV
CSVStorage storage = new CSVStorage();
storage.storeTable(new File("output.csv"),table,true);

```