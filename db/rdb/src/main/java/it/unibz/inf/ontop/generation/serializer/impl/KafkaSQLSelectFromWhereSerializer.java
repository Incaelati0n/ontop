package it.unibz.inf.ontop.generation.serializer.impl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import it.unibz.inf.ontop.dbschema.DBParameters;
import it.unibz.inf.ontop.generation.algebra.SelectFromWhereWithModifiers;
import it.unibz.inf.ontop.generation.serializer.SelectFromWhereSerializer;
import it.unibz.inf.ontop.model.term.TermFactory;

@Singleton
public class KafkaSQLSelectFromWhereSerializer extends DefaultSelectFromWhereSerializer implements SelectFromWhereSerializer {

    @Inject
    private KafkaSQLSelectFromWhereSerializer(TermFactory termFactory) {
        super(new DefaultSQLTermSerializer(termFactory) {
            @Override
            protected String serializeStringConstant(String constant) {
                // parent method + doubles backslashes
                return super.serializeStringConstant(constant)
                        .replace("\\", "\\\\");
            }
        });
    }

    @Override
    public QuerySerialization serialize(SelectFromWhereWithModifiers selectFromWhere, DBParameters dbParameters) {
        return selectFromWhere.acceptVisitor(new DefaultRelationVisitingSerializer(dbParameters.getQuotedIDFactory()) {

            /**
             * Not supported
             */
            @Override
            protected String serializeLimitOffset(long limit, long offset) {
                throw new UnsupportedOperationException("OFFSET clause not compliant to KafkaSQL syntax");
            }

            /**
             * Not supported
             */
            @Override
            protected String serializeOffset(long offset) {
                throw new UnsupportedOperationException("OFFSET clause not compliant to KafkaSQL syntax");
            }

        });
    }
}