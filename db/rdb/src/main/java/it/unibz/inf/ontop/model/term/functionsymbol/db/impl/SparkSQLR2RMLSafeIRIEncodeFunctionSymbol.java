package it.unibz.inf.ontop.model.term.functionsymbol.db.impl;

import it.unibz.inf.ontop.model.type.DBTermType;

public class SparkSQLR2RMLSafeIRIEncodeFunctionSymbol extends DefaultSQLEncodeURLorIRIFunctionSymbol {

    protected SparkSQLR2RMLSafeIRIEncodeFunctionSymbol(DBTermType dbStringType) {
        super(dbStringType, true);
    }

    /*
     * Backslash character is an escape symbol in SparkSQL dialect. Replace '\' --> '\\' to avoid malformed queries
     */
    @Override
    protected String encodeSQLStringConstant(String constant) {
        return super.encodeSQLStringConstant(constant.replace("\\", "\\\\"));
    }
}