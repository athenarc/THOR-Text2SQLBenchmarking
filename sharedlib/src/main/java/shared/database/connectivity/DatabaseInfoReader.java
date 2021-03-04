package shared.database.connectivity;

import shared.connectivity.thor.input.InputHandler;
import shared.database.config.PropertiesSingleton;
import shared.database.model.DatabaseType;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLIndexResult;
import shared.database.model.SQLTupleList;



// This class has the required functionality to get information
// about a database such as table names, column names and column types.
// It queries the INFORMATION_SCHEMA table.
public interface DatabaseInfoReader {

    // Gets all the information needed from the database.
    public static void getTableAndColumnNames(SQLDatabase database) {}
    public static void getFKConstraints(SQLDatabase database) {}
    public static void getIndexedColumns(SQLDatabase database) {}
    public static void getTableAndColumnStatistics(SQLDatabase database) {}
    


    // public static void main(String[] args) {
    //     PropertiesSingleton.loadPropertiesFile("app");
    //     SQLDatabase database = SQLDatabase.InstantiateDatabase("IMDB", DatabaseType.MySQL);
        
    //     // // Create indexes
    //     // System.out.println(database);

    //     InputHandler ih = new InputHandler();
    //     ih.readInput();
    //     System.out.println("DN: " + ih.getDatabaseName() + "\nDT: " + ih.getDatabaseType());

    //     // Search columns.
    //     // SQLIndexResult results = database.searchColumn(database.getTableByName("countries").getColumnByName("name"), "Spain");
    //     // SQLTupleList nl = new SQLTupleList(results.getTuples());
    //     // nl.print();
    // }
}
