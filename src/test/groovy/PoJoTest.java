import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.grl.tables.RowSerializer;
import com.grl.tables.SerializationException;
import com.grl.tables.Table;
import com.grl.tables.Table.Row;
import com.grl.tables.annotations.TableColumn;
import com.grl.tables.storage.CSVStorage;
import com.grl.tables.storage.JSONStorage;


public class PoJoTest {
	public static void main(String[] args) throws IOException{
		TestObj testObj = new TestObj();
		RowSerializer serializer = new RowSerializer();
		Table.Row row = new Table.Row(serializer.serialize(testObj));
		try {
			TestObj test2 = serializer.deserialize(TestObj.class, row);
			System.out.println(serializer.serialize(test2));
		} catch (SerializationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Table table = new Table();
		table.appendRow(row);
		
		new File("temp").mkdirs();
		
		CSVStorage storage = new CSVStorage();
		storage.storeTable(new File("temp/test.csv"), table, true);
		
		JSONStorage jsonStorage = new JSONStorage();
		jsonStorage.storeTable(table, new File("temp/test.json"));
		
		Table loadedTable = jsonStorage.loadTable(new File("temp/test.json"));
		for(Row loadedRow:loadedTable){
			for(String key:loadedRow.keySet()){
				System.out.print(String.format("%s=%s ",key,loadedRow.get(key)));
			}
			System.out.println("");
		}
		
	}
}

class TestObj{
	public static enum EnumTest{Value1,Value2};
	
	@TableColumn(name="Column Header")
	private String testValue = "Something";
	@TableColumn(name="Money", format="%.2f")
	private double money = 1.256;
	@TableColumn(name="NullValue")
	private String value = null;
	@TableColumn(name="EnumValue")
	private EnumTest enumValue = EnumTest.Value2;
}
