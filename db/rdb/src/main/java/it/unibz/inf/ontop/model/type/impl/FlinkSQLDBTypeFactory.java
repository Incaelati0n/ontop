package it.unibz.inf.ontop.model.type.impl;

import com.google.common.collect.ImmutableMap;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.model.type.*;
import it.unibz.inf.ontop.model.vocabulary.XSD;

import java.util.Map;

import static it.unibz.inf.ontop.model.type.DBTermType.Category.FLOAT_DOUBLE;
import static it.unibz.inf.ontop.model.type.DBTermType.Category.INTEGER;

public class FlinkSQLDBTypeFactory extends DefaultSQLDBTypeFactory {

    /**
     * FLINK-SQL 1.10 datatypes description : https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/table/types.html
     * remark: not everything supported as current flink jdbc-driver & gateway versions do f.e not support byte types.
     *
     * SQL to XML mappings : https://www.w3.org/2001/sw/rdb2rdf/wiki/Mapping_SQL_datatypes_to_XML_Schema_datatypes
     */

    protected static final String SHORT_STR = "SHORT";
    protected static final String LONG_STR = "LONG";
    protected static final String STRING_STR = "STRING";
    protected static final String DEC_STR = "DEC";

    @AssistedInject
    protected FlinkSQLDBTypeFactory(@Assisted TermType rootTermType, @Assisted TypeFactory typeFactory) {
        super(createFlinkSQLTypeMap(rootTermType, typeFactory), createFlinkSQLCodeMap());
    }

    private static Map<String, DBTermType> createFlinkSQLTypeMap(TermType rootTermType, TypeFactory typeFactory) {

        TermTypeAncestry rootAncestry = rootTermType.getAncestry();

        // Redefine the datatypes for numerical values
        RDFDatatype xsdShort = typeFactory.getDatatype(XSD.SHORT);
        RDFDatatype xsdInt = typeFactory.getDatatype(XSD.INT);
        RDFDatatype xsdLong = typeFactory.getDatatype(XSD.LONG);
        RDFDatatype xsdFloat = typeFactory.getDatatype(XSD.FLOAT);

        DBTermType shortType = new NumberDBTermType(SHORT_STR, rootAncestry, xsdShort, INTEGER);
        DBTermType intType = new NumberDBTermType(INT_STR, rootAncestry, xsdInt, INTEGER);
        DBTermType longType = new NumberDBTermType(LONG_STR, rootAncestry, xsdLong, INTEGER);
        DBTermType floatType = new NumberDBTermType(FLOAT_STR, rootAncestry, xsdFloat, FLOAT_DOUBLE);
        DBTermType stringType = new StringDBTermType(STRING_STR, rootAncestry, typeFactory.getXsdStringDatatype());

        Map<String, DBTermType> map = createDefaultSQLTypeMap(rootTermType, typeFactory);
        map.put(STRING_STR, stringType);
        map.put(SHORT_STR,shortType);
        map.put(SMALLINT_STR,shortType);
        map.put(INT_STR,intType);
        map.put(INTEGER_STR,intType);
        map.put(BIGINT_STR,longType);
        map.put(FLOAT_STR,floatType);
        map.put(DEC_STR, map.get(DECIMAL_STR));
        return map;
    }

    private static ImmutableMap<DefaultTypeCode, String> createFlinkSQLCodeMap() {
        Map<DefaultTypeCode, String> map = createDefaultSQLCodeMap();
        map.put(DefaultTypeCode.STRING, STRING_STR);
        map.put(DefaultTypeCode.HEXBINARY,BINARY_STR);
        return ImmutableMap.copyOf(map);
    }

}