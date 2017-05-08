package org.flywaydb.core.internal.dbsupport.impala;

import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.SqlStatementBuilder;
import org.flywaydb.core.internal.util.StringUtils;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by niknyl on 05/05/2017.
 */
public class ImpalaDbSupport extends DbSupport {

    private static final Log LOG = LogFactory.getLog(ImpalaDbSupport.class);

    public ImpalaDbSupport(Connection connection) {
        super(new JdbcTemplate(connection, Types.VARCHAR));
    }

    @Override
    public Schema getSchema(String name) {
        return new ImpalaSchema(jdbcTemplate, this, "paf");
    }

    @Override
    public SqlStatementBuilder createSqlStatementBuilder() {
        return new ImpalaSqlStatementBuilder();
    }

    @Override
    public String getDbName() {
        return "impala";
    }

    @Override
    protected String doGetCurrentSchemaName() throws SQLException {
        return jdbcTemplate.getConnection().getCatalog();
    }

    @Override
    protected void doChangeCurrentSchemaTo(String schema) throws SQLException {
        if (!StringUtils.hasLength(schema)) {
            try {
                jdbcTemplate.execute("USE default");
            } catch (Exception e) {
                LOG.warn("Unable to restore connection to having no default schema: " + e.getMessage());
            }
        } else {
            jdbcTemplate.getConnection().setCatalog(schema);
        }
    }

    @Override
    public String getCurrentUserFunction() {
        return "user()";
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public String getBooleanTrue() {
        return "1";
    }

    @Override
    public String getBooleanFalse() {
        return "0";
    }

    @Override
    protected String doQuote(String identifier) {
        return identifier;
    }

    @Override
    public boolean catalogIsSchema() {
        return true;
    }
}
