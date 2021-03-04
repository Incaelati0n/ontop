package it.unibz.inf.ontop.dbschema.impl;

import com.google.common.collect.ImmutableList;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.exception.MetadataExtractionException;
import it.unibz.inf.ontop.exception.RelationNotFoundInMetadataException;
import it.unibz.inf.ontop.injection.CoreSingletons;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.model.type.DBTypeFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

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

    @Override
    public NamedRelationDefinition getRelation(RelationID id0) throws MetadataExtractionException {
        DBTypeFactory dbTypeFactory = dbParameters.getDBTypeFactory();
        RelationID id = getCanonicalRelationId(id0);
        try (ResultSet rs = metadata.getColumns(getRelationCatalog(id), getRelationSchema(id), getRelationName(id), null)) {
            Map<RelationID, RelationDefinition.AttributeListBuilder> relations = new HashMap<>();

            while (rs.next()) {
                RelationID extractedId = getRelationID(rs, "TABLE_CAT", "TABLE_SCHEM","TABLE_NAME");
                checkSameRelationID(extractedId, id);

                RelationDefinition.AttributeListBuilder builder = relations.computeIfAbsent(extractedId,
                        i -> DatabaseTableDefinition.attributeListBuilder());

                QuotedID attributeId = rawIdFactory.createAttributeID(rs.getString("COLUMN_NAME"));
                // columnNoNulls, columnNullable, columnNullableUnknown
                boolean isNullable = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;
                String typeName = rs.getString("TYPE_NAME");
                int columnSize = rs.getInt("COLUMN_SIZE");
                DBTermType termType = dbTypeFactory.getDBTermType(typeName, columnSize);

                String sqlTypeName;
                switch (rs.getInt("DATA_TYPE")) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                        sqlTypeName = (columnSize != 0) ? typeName + "(" + columnSize + ")" : typeName;
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        int decimalDigits = 0;
                        if (columnSize == 0)
                            sqlTypeName = typeName;
                        else if (decimalDigits == 0)
                            sqlTypeName = typeName + "(" + columnSize + ")";
                        else
                            sqlTypeName = typeName + "(" + columnSize + ", " + decimalDigits + ")";
                        break;
                    default:
                        sqlTypeName = typeName;
                }
                builder.addAttribute(attributeId, termType, sqlTypeName, isNullable);
            }

            if (relations.entrySet().size() == 1) {
                Map.Entry<RelationID, RelationDefinition.AttributeListBuilder> r = relations.entrySet().iterator().next();
                return new DatabaseTableDefinition(getAllIDs(r.getKey()), r.getValue());
            }
            throw relations.isEmpty()
                    ? new RelationNotFoundInMetadataException(id, getRelationIDs())
                    : new MetadataExtractionException("Cannot resolve ambiguous relation id: " + id + ": " + relations.keySet());
        }
        catch (SQLException e) {
            throw new MetadataExtractionException(e);
        }
    }
}