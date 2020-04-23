import Model.IQuery;
import QueryParser.DatabaseHelper;
import common.CatalogDB;
import common.Constants;

import java.io.File;
import java.util.Scanner;

public class UserPrompt {

  private static boolean isExit = false;
  private static Scanner scanner = new Scanner(System.in).useDelimiter(";");
  private static final String USE_HELP_MESSAGE = "Please use 'help' to see a list of commands";

    public static void main(String[] args) {

        INITDB();
		startScreen();

        while(!isExit) {
            System.out.print("davisql> ");
            String userCommand = scanner.next().replace("\n", "").replace("\r", " ").trim().toLowerCase();
            parseUserCommand(userCommand);
        }
    }

    private static void startScreen() {
        System.out.println("Initializing DavisBase.\n\n"); 
        System.out.println("Type 'help;' for more help.");
        System.out.println("*************************************************************************************");
    }

    private static void parseUserCommand (String userCommand) {
        if(userCommand.toLowerCase().equals(DatabaseHelper.SHOW_TABLES_COMMAND.toLowerCase())){
            IQuery query = DatabaseHelper.ShowTable();
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().equals(DatabaseHelper.SHOW_DATABASES_COMMAND.toLowerCase())){
            IQuery query = DatabaseHelper.ShowDatabase();
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().equals(DatabaseHelper.HELP_COMMAND.toLowerCase())){
            DatabaseHelper.HelpQueryHandler();
        }
        else if(userCommand.toLowerCase().equals(DatabaseHelper.EXIT_COMMAND.toLowerCase())){
            System.out.println("Exit Database");
            isExit = true;
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.USE_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.USE_DATABASE_COMMAND)){
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(DatabaseHelper.USE_DATABASE_COMMAND.length());
            IQuery query = DatabaseHelper.UseDatabase(databaseName.trim());
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.DESCRIBE_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.DESCRIBE_TABLE_COMMAND)) {
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String tableName;
            tableName = userCommand.substring(DatabaseHelper.DESCRIBE_TABLE_COMMAND.length());
            IQuery query = DatabaseHelper.DescTableQueryHandler(tableName.trim());
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.DROP_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.DROP_TABLE_COMMAND)){
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String tableName = userCommand.substring(DatabaseHelper.DROP_TABLE_COMMAND.length());
            IQuery query = DatabaseHelper.DropTable(tableName.trim());
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.DROP_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.DROP_DATABASE_COMMAND)){
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(DatabaseHelper.DROP_DATABASE_COMMAND.length());
            IQuery query = DatabaseHelper.DropDatabase(databaseName.trim());
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.SELECT_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.SELECT_COMMAND)){
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            int index = userCommand.toLowerCase().indexOf("from");
            if(index == -1) {
                DatabaseHelper.UnknownCommand(userCommand, "Expected FROM keyword");
                return;
            }

            String attributeList = userCommand.substring(DatabaseHelper.SELECT_COMMAND.length(), index).trim();
            String restUserQuery = userCommand.substring(index + "from".length());

            index = restUserQuery.toLowerCase().indexOf("where");
            if(index == -1) {
                String tableName = restUserQuery.trim();
                IQuery query = DatabaseHelper.SelectQueryHandler(attributeList.split(","), tableName, "");
                DatabaseHelper.ExecuteQuery(query);
                return;
            }

            String tableName = restUserQuery.substring(0, index);
            String conditions = restUserQuery.substring(index + "where".length());
            IQuery query = DatabaseHelper.SelectQueryHandler(attributeList.split(","), tableName.trim(), conditions);
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.INSERT_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.INSERT_COMMAND)){
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String tableName = "";
            String columns = "";

            int valuesIndex = userCommand.toLowerCase().indexOf("values");
            if(valuesIndex == -1) {
                DatabaseHelper.UnknownCommand(userCommand, "Expected VALUES keyword");
                return;
            }

            String columnOptions = userCommand.toLowerCase().substring(0, valuesIndex);
            int openBracketIndex = columnOptions.indexOf("(");

            if(openBracketIndex != -1) {
                tableName = userCommand.substring(DatabaseHelper.INSERT_COMMAND.length(), openBracketIndex).trim();
                int closeBracketIndex = userCommand.indexOf(")");
                if(closeBracketIndex == -1) {
                    DatabaseHelper.UnknownCommand(userCommand, "Expected ')'");
                    return;
                }

                columns = userCommand.substring(openBracketIndex + 1, closeBracketIndex).trim();
            }

            if(tableName.equals("")) {
                tableName = userCommand.substring(DatabaseHelper.INSERT_COMMAND.length(), valuesIndex).trim();
            }

            String valuesList = userCommand.substring(valuesIndex + "values".length()).trim();
            if(!valuesList.startsWith("(")){
                DatabaseHelper.UnknownCommand(userCommand, "Expected '('");
                return;
            }

            if(!valuesList.endsWith(")")){
                DatabaseHelper.UnknownCommand(userCommand, "Expected ')'");
                return;
            }

            valuesList = valuesList.substring(1, valuesList.length()-1);
            IQuery query = DatabaseHelper.InsertQueryHandler(tableName, columns, valuesList);
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.DELETE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.DELETE_COMMAND)){
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String tableName = "";
            String condition = "";
            int index = userCommand.toLowerCase().indexOf("where");
            if(index == -1) {
                tableName = userCommand.substring(DatabaseHelper.DELETE_COMMAND.length()).trim();
                IQuery query = DatabaseHelper.DeleteQuery(tableName, condition);
                DatabaseHelper.ExecuteQuery(query);
                return;
            }

            if(tableName.equals("")) {
                tableName = userCommand.substring(DatabaseHelper.DELETE_COMMAND.length(), index).trim();
            }

            condition = userCommand.substring(index + "where".length());
            IQuery query = DatabaseHelper.DeleteQuery(tableName, condition);
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.UPDATE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.UPDATE_COMMAND)){
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String conditions = "";
            int setIndex = userCommand.toLowerCase().indexOf("set");
            if(setIndex == -1) {
                DatabaseHelper.UnknownCommand(userCommand, "Expected SET keyword");
                return;
            }

            String tableName = userCommand.substring(DatabaseHelper.UPDATE_COMMAND.length(), setIndex).trim();
            String clauses = userCommand.substring(setIndex + "set".length());
            int whereIndex = userCommand.toLowerCase().indexOf("where");
            if(whereIndex == -1){
                IQuery query = DatabaseHelper.UpdateQuery(tableName, clauses, conditions);
                DatabaseHelper.ExecuteQuery(query);
                return;
            }

            clauses = userCommand.substring(setIndex + "set".length(), whereIndex).trim();
            conditions = userCommand.substring(whereIndex + "where".length());
            IQuery query = DatabaseHelper.UpdateQuery(tableName, clauses, conditions);
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.CREATE_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.CREATE_DATABASE_COMMAND)){
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(DatabaseHelper.CREATE_DATABASE_COMMAND.length());
            IQuery query = DatabaseHelper.CreateDatabase(databaseName.trim());
            DatabaseHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DatabaseHelper.CREATE_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DatabaseHelper.CREATE_TABLE_COMMAND)){
                DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            int openBracketIndex = userCommand.toLowerCase().indexOf("(");
            if(openBracketIndex == -1) {
                QueryParser.DatabaseHelper.UnknownCommand(userCommand, "Expected (");
                return;
            }

            if(!userCommand.endsWith(")")){
                QueryParser.DatabaseHelper.UnknownCommand(userCommand, "Missing )");
                return;
            }

            String tableName = userCommand.substring(DatabaseHelper.CREATE_TABLE_COMMAND.length(), openBracketIndex).trim();
            String columnsPart = userCommand.substring(openBracketIndex + 1, userCommand.length()-1);
            IQuery query = DatabaseHelper.CreateTableQueryHandler(tableName, columnsPart);
            DatabaseHelper.ExecuteQuery(query);
        }
        else{
            DatabaseHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
        }
    }

    private static boolean PartsEqual(String userCommand, String expectedCommand) {
        String[] userParts = userCommand.toLowerCase().split(" ");
        String[] actualParts = expectedCommand.toLowerCase().split(" ");

        for(int i=0;i<actualParts.length;i++){
            if(!actualParts[i].equals(userParts[i])){
                return false;
            }
        }

        return true;
    }

    private static void INITDB() {
        File baseDir = new File(Constants.DEFAULT_DATA_DIRNAME);
        if(!baseDir.exists()) {
            File catalogDir = new File(Constants.DEFAULT_DATA_DIRNAME + "/" + Constants.DEFAULT_CATALOG_DATABASENAME);
            if(!catalogDir.exists()) {
                if(catalogDir.mkdirs()) {
                    new CatalogDB().createCatalogDB();
                }
            }
        }

    }
}