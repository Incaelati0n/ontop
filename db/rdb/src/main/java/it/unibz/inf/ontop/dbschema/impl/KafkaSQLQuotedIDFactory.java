package it.unibz.inf.ontop.dbschema.impl;

import it.unibz.inf.ontop.dbschema.QuotedID;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Creates QuotedIdentifiers following the rules of KafkaSQL:
 *    - double and single quotes are not tolerated for schema and attributes definition
 */

public class KafkaSQLQuotedIDFactory extends SQLStandardQuotedIDFactory {

    private static final String SQL_QUOTATION_STRING = "`";
    private final boolean caseSensitiveTableNames;

    KafkaSQLQuotedIDFactory(boolean caseSensitiveTableNames) {
        this.caseSensitiveTableNames = caseSensitiveTableNames;
    }

    protected QuotedID createFromString(@Nonnull String s) {
        Objects.requireNonNull(s);

        // Backticks are tolerated for KafkaSQL schema and table names, but not necessary
        if (s.startsWith(SQL_QUOTATION_STRING) && s.endsWith(SQL_QUOTATION_STRING))
            return new QuotedIDImpl(s.substring(1, s.length() - 1), SQL_QUOTATION_STRING, caseSensitiveTableNames);

        return new QuotedIDImpl(s, SQL_QUOTATION_STRING, caseSensitiveTableNames);
    }

    @Override
    public String getIDQuotationString() {
        return SQL_QUOTATION_STRING;
    }
}