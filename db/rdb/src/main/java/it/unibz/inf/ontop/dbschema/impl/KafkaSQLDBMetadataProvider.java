package it.unibz.inf.ontop.dbschema.impl;

import com.google.common.collect.ImmutableList;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.exception.MetadataExtractionException;
import it.unibz.inf.ontop.injection.CoreSingletons;

import java.sql.*;

import static it.unibz.inf.ontop.dbschema.RelationID.TABLE_INDEX;

/*
*   open issue: https://github.com/JSQLParser/JSqlParser/issues/1126
*   Description: because JSQLParser does not (yet) support the construct of "EMIT CHANGES",
		         it is not possible to query un-materialized tables. This brings some limitations with
		         it, as we can only query materialized tables having already aggregated data in them.
*   Workaround until they support expression: Materialize tables by hand and query the materialized table instead.
*   Limitations: Materialized tables contain aggregated data from the main table, which
			     makes them hardly usable for real-world implementation and use as you can not
			     query full tables only the pre-defined aggregated version of it.
*
 */
public class KafkaSQLDBMetadataProvider extends AbstractDBMetadataProvider{

    @AssistedInject
    KafkaSQLDBMetadataProvider(@Assisted Connection connection, CoreSingletons coreSingletons) throws MetadataExtractionException {
        super(connection, metadata -> new KafkaSQLQuotedIDFactory(false), coreSingletons);
    }

    @Override
    public void insertIntegrityConstraints(NamedRelationDefinition relation, MetadataLookup metadataLookup) throws MetadataExtractionException {
        // java.sql.SQLFeatureNotSupportedException: Feature not supported: getPrimaryKeys
    }

    @Override
    protected RelationID getCanonicalRelationId(RelationID id) { return id; }

    @Override
    protected ImmutableList<RelationID> getAllIDs(RelationID id) { return ImmutableList.of(id); }

    @Override
    protected String getRelationCatalog(RelationID relationID) { return null; }

    @Override
    protected String getRelationSchema(RelationID relationID) { return null; }

    @Override
    protected String getRelationName(RelationID relationID) { return relationID.getComponents().get(TABLE_INDEX).getName(); }

    @Override
    protected RelationID getRelationID(ResultSet rs, String catalogNameColumn, String schemaNameColumn, String tableNameColumn) throws SQLException {
        return rawIdFactory.createRelationID(rs.getString(tableNameColumn));
    }
}