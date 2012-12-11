import java.io.File;
import java.io.IOException;

import com.grl.tables.Table;
import com.grl.tables.Table.Row;
import com.grl.tables.annotations.TableColumn;
import com.grl.tables.storage.CSVStorage;
import com.grl.tables.storage.JSONStorage;


public class PoJoTest {
	public static void main(String[] args) throws IOException{
		Object testObj = new Object(){
			@TableColumn(name="Column Header")
			private String testValue = "Something";
		};
		Table.Row row = new Table.Row(testObj);
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
