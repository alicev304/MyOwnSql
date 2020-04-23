package QueryParser;

import Model.*;
import queries.*;

import java.io.File;
import java.util.ArrayList;

public class DatabaseHelper {

    public static final String SELECT_COMMAND = "SELECT";
    public static final String DROP_TABLE_COMMAND = "DROP TABLE";
    public static final String DROP_DATABASE_COMMAND = "DROP DATABASE";
    public static final String HELP_COMMAND = "HELP";
    public static final String VERSION_COMMAND = "VERSION";
    public static final String EXIT_COMMAND = "EXIT";
    public static final String SHOW_TABLES_COMMAND = "SHOW TABLES";
    public static final String SHOW_DATABASES_COMMAND = "SHOW DATABASES";
    public static final String INSERT_COMMAND = "INSERT INTO";
    public static final String DELETE_COMMAND = "DELETE FROM";
    public static final String UPDATE_COMMAND = "UPDATE";
    public static final String CREATE_TABLE_COMMAND = "CREATE TABLE";
    public static final String CREATE_DATABASE_COMMAND = "CREATE DATABASE";
    public static final String USE_DATABASE_COMMAND = "USE";
    public static final String DESCRIBE_TABLE_COMMAND = "DESCRIBE";
    private static final String NO_DATABASE_SELECTED_MESSAGE = "No database selected";

    public static String CurrentDatabaseName = "";

    public static IQuery ShowTable() {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new ShowTableQuery(DatabaseHelper.CurrentDatabaseName);
    }

    public static IQuery DropTable(String tableName) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new DropTableQuery(DatabaseHelper.CurrentDatabaseName, tableName);
    }

    public static void UnknownCommand(String userCommand, String message) {
        System.out.println("Unrecognized Command " + userCommand);
        System.out.println("Message : " + message);
    }

    public static IQuery SelectQueryHandler(String[] attributes, String tableName, String conditionString) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        boolean isSelectAll = false;
        SelectQuery query;
        ArrayList<String> columns = new ArrayList<>();
        for(String attribute : attributes){
            columns.add(attribute.trim());
        }

        if(columns.size() == 1 && columns.get(0).equals("*")) {
            isSelectAll = true;
            columns = null;
        }

        if(conditionString.equals("")){
            query = new SelectQuery(DatabaseHelper.CurrentDatabaseName, tableName, columns, null, isSelectAll);
            return query;
        }

        Condition condition = Condition.CreateCondition(conditionString);
        if(condition == null) return null;

        ArrayList<Condition> conditionList = new ArrayList<>();
        conditionList.add(condition);
        query = new SelectQuery(DatabaseHelper.CurrentDatabaseName, tableName, columns, conditionList, isSelectAll);
        return query;
    }

    public static void HelpQueryHandler() {
    	System.out.println("*************************************************************************************");
        System.out.println("All commands end with ';' and are case insensetive.\n");
        System.out.println("SHOW DATABASES;                                  Show all atabases.");
        System.out.println("USE DATABASE database_name;                      Select/change database.");
        System.out.println("CREATE DATABASE database_name;                   Creates a database.");
        System.out.println("DROP DATABASE database_name;                     Remove database.");
        System.out.println("SHOW TABLES;                                     Show all tables in the database.");
        System.out.println("DESCRIBE table_name;                             Show table schema.");
        System.out.println("CREATE TABLE table_name (schema);                Creates a table in the database.");
        System.out.println("DROP TABLE table_name;                           Delete table data");
        System.out.println("SELECT                                           Begins SELECT clause.");
        System.out.println("INSERT INTO table_name                           Begins INSERT clause.");
        System.out.println("DELETE FROM table_name                           Begins DELETE clause.");
        System.out.println("UPDATE table_name SET                            Begins UPDATE clause.");
        System.out.println("HELP;                                            Display help.");
        System.out.println("EXIT;                                            Exit the program\n");
        System.out.println("*************************************************************************************");
    }

    public static String line(String s,int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }

    public static IQuery InsertQueryHandler(String tableName, String columnsString, String valuesList) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;
        ArrayList<String> columns = null;
        ArrayList<Literal> values = new ArrayList<>();

        if(!columnsString.equals("")) {
            columns = new ArrayList<>();
            String[] columnList = columnsString.split(",");
            for(String column : columnList){
                columns.add(column.trim());
            }
        }

        for(String value : valuesList.split(",")){
            Literal literal = Literal.CreateLiteral(value.trim());
            if(literal == null) return null;
            values.add(literal);
        }

        if(columns != null && columns.size() != values.size()){
            DatabaseHelper.UnknownCommand("", "Number of columns and values don't match");
            return null;
        }

        query = new InsertQuery(DatabaseHelper.CurrentDatabaseName, tableName, columns, values);
        return query;
    }

    public static IQuery DeleteQuery(String tableName, String conditionString) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;

        if(conditionString.equals("")){
            query = new DeleteQuery(DatabaseHelper.CurrentDatabaseName, tableName, null);
            return query;
        }

        Condition condition = Condition.CreateCondition(conditionString);
        if(condition == null) return null;

        query = new DeleteQuery(DatabaseHelper.CurrentDatabaseName, tableName, condition);
        return query;
    }

    public static IQuery UpdateQuery(String tableName, String clauseString, String conditionString) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;

        Condition clause = Condition.CreateCondition(clauseString);
        if(clause == null) return null;

        if(clause.operator != Operator.EQUALS){
            DatabaseHelper.UnknownCommand(clauseString, "SET clause should only contain = operator");
            return null;
        }

        if(conditionString.equals("")){
            query = new UpdateQuery(DatabaseHelper.CurrentDatabaseName, tableName, clause.column, clause.value, null);
            return query;
        }

        Condition condition = Condition.CreateCondition(conditionString);
        if(condition == null) return null;

        query = new UpdateQuery(DatabaseHelper.CurrentDatabaseName, tableName, clause.column, clause.value, condition);
        return query;
    }

    public static IQuery CreateTableQueryHandler(String tableName, String columnsPart) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;
        boolean hasPrimaryKey = false;
        ArrayList<Column> columns = new ArrayList<>();
        String[] columnsList = columnsPart.split(",");

        for(String columnEntry : columnsList){
            Column column = Column.CreateColumn(columnEntry.trim());
            if(column == null) return null;
            columns.add(column);
        }

        if(columnsList.length > 0 && columnsList[0].toLowerCase().endsWith("primary key")){
            if(columns.get(0).type == DataType.INT){
                hasPrimaryKey = true;
            }
            else{
                DatabaseHelper.UnknownCommand(columnsList[0], "PRIMARY KEY has to have INT datatype");
                return null;
            }

        }

        query = new CreateTableQuery(DatabaseHelper.CurrentDatabaseName, tableName, columns, hasPrimaryKey);
        return query;
    }

    public static IQuery DropDatabase(String databaseName) {
        return new DropDatabaseQuery(databaseName);
    }

    public static IQuery ShowDatabase() {
        return new ShowDatabaseQuery();
    }

    public static IQuery UseDatabase(String databaseName) {
        return new UseDatabaseQuery(databaseName);
    }

    public static IQuery CreateDatabase(String databaseName) {
        return new CreateDatabaseQuery(databaseName);
    }

    public static boolean DatabaseExists(String databaseName) {
        String DEFAULT_DATA_DIRNAME = "data";
        File dirFile = new File(DEFAULT_DATA_DIRNAME+ "/" + databaseName);
        return dirFile.exists();
    }

    public static boolean TableExists(String databaseName, String tableName) {
        String DEFAULT_DATA_DIRNAME = "data";
        String DEFAULT_TABLE_EXTENSION = "tbl";

        File tableFile = new File(String.format("%s/%s/%s.%s", DEFAULT_DATA_DIRNAME, databaseName, tableName, DEFAULT_TABLE_EXTENSION));
        return tableFile.exists();
    }

    public static void ExecuteQuery(IQuery query) {
        if(query!=null && query.ValidateQuery()){
            Result result = query.ExecuteQuery();
            if(result != null){
                result.Display();
            }
        }
    }

    public static IQuery DescTableQueryHandler(String tableName) {
        if(DatabaseHelper.CurrentDatabaseName.equals("")){
            System.out.println(DatabaseHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new DescTableQuery(DatabaseHelper.CurrentDatabaseName, tableName);
    }
}
