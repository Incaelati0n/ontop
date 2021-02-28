package it.unibz.inf.ontop.answering.connection.impl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import it.unibz.inf.ontop.injection.OntopSystemSQLSettings;

import java.sql.SQLException;
import java.sql.Statement;

/**
 *  Comment: setFetchSize currently not supported in KSQL driver.
 */
@Singleton
public class KafkaJDBCStatementInitializer extends DefaultJDBCStatementInitializer {

    @Inject
    protected KafkaJDBCStatementInitializer(OntopSystemSQLSettings settings) {
        super(settings);
    }

    @Override
    protected Statement init(Statement statement) throws SQLException {
        int fetchSize = 0;
        if (fetchSize > 0)
            statement.setFetchSize(fetchSize);
        return statement;
    }
}
