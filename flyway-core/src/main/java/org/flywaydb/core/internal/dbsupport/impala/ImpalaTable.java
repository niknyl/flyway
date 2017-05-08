package org.flywaydb.core.internal.dbsupport.impala;

import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.Table;

import java.sql.SQLException;

/**
 * Created by niknyl on 05/05/2017.
 */
public class ImpalaTable extends Table {

    public ImpalaTable(JdbcTemplate jdbcTemplate, DbSupport dbSupport, Schema schema, String name) {
        super(jdbcTemplate, dbSupport, schema, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForList("show tables in " + schema.getName() + " like '" + name + "'").size() > 0;
    }

    @Override
    protected void doLock() throws SQLException {

    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("drop table " + dbSupport.quote(schema.getName(), name));
    }

    @Override
    public String toString() {
        return schema.getName() + "." + name;
    }
}
