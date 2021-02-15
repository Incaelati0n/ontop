package it.unibz.inf.ontop.dbschema.impl;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.dbschema.MetadataLookup;
import it.unibz.inf.ontop.dbschema.NamedRelationDefinition;
import it.unibz.inf.ontop.dbschema.RelationID;
import it.unibz.inf.ontop.exception.MetadataExtractionException;
import it.unibz.inf.ontop.injection.CoreSingletons;

import java.sql.Connection;
import java.sql.SQLException;

public class FlinkSQLDBMetadataProvider extends DefaultSchemaDBMetadataProvider{

    @AssistedInject
    FlinkSQLDBMetadataProvider(@Assisted Connection connection, CoreSingletons coreSingletons) throws MetadataExtractionException {
        super(connection, metadata -> new FlinkSQLQuotedIDFactory(false), coreSingletons);
    }

    @Override
    protected String getRelationCatalog(RelationID id) { return "default_catalog"; }

    @Override
    protected String getRelationSchema(RelationID id) { return "default_database"; }

    @Override
    protected String getRelationName(RelationID id) { return "test"; }

    @Override
    public void insertIntegrityConstraints(NamedRelationDefinition relation, MetadataLookup metadataLookup) throws MetadataExtractionException {
        try {
            insertPrimaryKey(relation);
            /* try and get to work */
            //insertUniqueAttributes(relation);
            //insertForeignKeys(relation, metadataLookup);
        }
        catch (SQLException e) {
            throw new MetadataExtractionException(e);
        }
    }
}