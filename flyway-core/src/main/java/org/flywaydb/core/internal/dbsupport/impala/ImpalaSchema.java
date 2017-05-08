package org.flywaydb.core.internal.dbsupport.impala;

import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.Table;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by niknyl on 05/05/2017.
 */
public class ImpalaSchema extends Schema<ImpalaDbSupport> {

    public ImpalaSchema(JdbcTemplate jdbcTemplate, ImpalaDbSupport dbSupport, String name) {
        super(jdbcTemplate, dbSupport, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForList("show schemas like '" + name + "'").size() > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return jdbcTemplate.queryForList("show tables in " + name).size() == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("create schema " + dbSupport.quote(name));

    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("drop schema " + dbSupport.quote(name));

    }

    @Override
    protected void doClean() throws SQLException {

    }

    @Override
    protected Table[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList("show tables in " + name);

        Table[] tables = new Table[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new ImpalaTable(jdbcTemplate, dbSupport, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new ImpalaTable(jdbcTemplate, dbSupport, this, tableName);
    }
}
