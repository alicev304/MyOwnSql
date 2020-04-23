package queries;

import Model.Condition;
import Model.IQuery;
import Model.Result;
import Model.ResultSet;
import common.CatalogDB;
import common.Constants;

import java.util.ArrayList;

public class ShowTableQuery implements IQuery {

    public String databaseName;

    public ShowTableQuery(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("table_name");

        Condition condition = Condition.CreateCondition(String.format("database_name = '%s'", this.databaseName));
        ArrayList<Condition> conditionList = new ArrayList<>();
        conditionList.add(condition);

        IQuery query = new SelectQuery(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, columns, conditionList, false);
        ResultSet resultSet = (ResultSet) query.ExecuteQuery();
        return resultSet;
    }

    @Override
    public boolean ValidateQuery() {
        return true;
    }
}
