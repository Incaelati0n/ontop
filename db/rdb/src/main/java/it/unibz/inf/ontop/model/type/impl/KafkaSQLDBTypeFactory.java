package it.unibz.inf.ontop.model.type.impl;

import com.google.common.collect.ImmutableMap;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.model.type.*;
import it.unibz.inf.ontop.model.vocabulary.XSD;

import java.util.Map;

import static it.unibz.inf.ontop.model.type.DBTermType.Category.FLOAT_DOUBLE;
import static it.unibz.inf.ontop.model.type.DBTermType.Category.INTEGER;

public class KafkaSQLDBTypeFactory extends DefaultSQLDBTypeFactory {

    /**
     * Kafka-SQL 5.5.0 datatypes description : https://docs.confluent.io/5.4.0/ksql/docs/developer-guide/syntax-reference.html
     *
     * SQL to XML mappings : https://www.w3.org/2001/sw/rdb2rdf/wiki/Mapping_SQL_datatypes_to_XML_Schema_datatypes
     */

    protected static final String STRING_STR = "STRING";
    protected static final String STRING_ARRAY = "ARRAY";

    @AssistedInject
    protected KafkaSQLDBTypeFactory(@Assisted TermType rootTermType, @Assisted TypeFactory typeFactory) {
        super(createKafkaSQLTypeMap(rootTermType, typeFactory), createKafkaSQLCodeMap());
    }

    private static Map<String, DBTermType> createKafkaSQLTypeMap(TermType rootTermType, TypeFactory typeFactory) {

        TermTypeAncestry rootAncestry = rootTermType.getAncestry();

        DBTermType stringType = new StringDBTermType(STRING_STR, rootAncestry, typeFactory.getXsdStringDatatype());

        Map<String, DBTermType> map = createDefaultSQLTypeMap(rootTermType, typeFactory);
        map.put(STRING_STR, stringType);
        return map;
    }

    private static ImmutableMap<DefaultTypeCode, String> createKafkaSQLCodeMap() {
        Map<DefaultTypeCode, String> map = createDefaultSQLCodeMap();
        map.put(DefaultTypeCode.STRING, STRING_STR);
        map.put(DefaultTypeCode.HEXBINARY,BINARY_STR);
        return ImmutableMap.copyOf(map);
    }

}