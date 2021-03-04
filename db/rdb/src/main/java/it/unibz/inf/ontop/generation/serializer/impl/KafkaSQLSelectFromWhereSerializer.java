package it.unibz.inf.ontop.generation.serializer.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.generation.algebra.*;
import it.unibz.inf.ontop.generation.serializer.SelectFromWhereSerializer;
import it.unibz.inf.ontop.model.term.ImmutableTerm;
import it.unibz.inf.ontop.model.term.TermFactory;
import it.unibz.inf.ontop.model.term.Variable;
import it.unibz.inf.ontop.substitution.ImmutableSubstitution;
import it.unibz.inf.ontop.utils.ImmutableCollectors;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    /**
     * Mutable: one instance per SQL query to generate
     */
    protected class DefaultRelationVisitingSerializer implements SQLRelationVisitor<QuerySerialization> {

        private static final String VIEW_PREFIX = "v";
        private static final String SELECT_FROM_WHERE_MODIFIERS_TEMPLATE = "SELECT %s%s\nFROM %s\n%s%s%s%s";

        protected final QuotedIDFactory idFactory;

        private final AtomicInteger viewCounter;

        protected DefaultRelationVisitingSerializer(QuotedIDFactory idFactory) {
            this.idFactory = idFactory;
            this.viewCounter = new AtomicInteger(0);
        }

        @Override
        public QuerySerialization visit(SelectFromWhereWithModifiers selectFromWhere) {

            QuerySerialization fromQuerySerialization = getSQLSerializationForChild(selectFromWhere.getFromSQLExpression());

            ImmutableMap<Variable, QuotedID> variableAliases = createVariableAliases(selectFromWhere.getProjectedVariables());

            String distinctString = "";

            ImmutableMap<Variable, QualifiedAttributeID> columnIDs = fromQuerySerialization.getColumnIDs();
            String projectionString = serializeProjection(selectFromWhere.getProjectedVariables(),
                    variableAliases, selectFromWhere.getSubstitution(), columnIDs);

            String fromString = fromQuerySerialization.getString();

            // Kafka replace IS NULL, IS NOT NULL parts
            String whereString = selectFromWhere.getWhereExpression()
                    .map(e -> sqlTermSerializer.serialize(e, columnIDs))
                    //.map(s -> String.format("WHERE %s\n", s))
                    .map(s -> String.format("WHERE %s\n", s.replaceAll("\\(.* NULL AND", "(")))
                    .orElse("");

            String groupByString = serializeGroupBy(selectFromWhere.getGroupByVariables(), columnIDs);
            String orderByString = serializeOrderBy(selectFromWhere.getSortConditions(), columnIDs);
            String sliceString = serializeSlice(selectFromWhere.getLimit(), selectFromWhere.getOffset());

            String sql = String.format(SELECT_FROM_WHERE_MODIFIERS_TEMPLATE, distinctString, projectionString,
                    fromString, whereString, groupByString, orderByString, sliceString);

            // Creates an alias for this SQLExpression and uses it for the projected columns
            RelationID alias = generateFreshViewAlias();
            return new QuerySerializationImpl(sql, attachRelationAlias(alias, variableAliases));
        }

        protected RelationID generateFreshViewAlias() {
            return idFactory.createRelationID(VIEW_PREFIX + viewCounter.incrementAndGet());
        }

        private ImmutableMap<Variable, QualifiedAttributeID> attachRelationAlias(RelationID alias, ImmutableMap<Variable, QuotedID> variableAliases) {
            return variableAliases.entrySet().stream()
                    .collect(ImmutableCollectors.toMap(
                            Map.Entry::getKey,
                            e -> new QualifiedAttributeID(alias, e.getValue())));
        }

        private ImmutableMap<Variable, QualifiedAttributeID> replaceRelationAlias(RelationID alias, ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
            return columnIDs.entrySet().stream()
                    .collect(ImmutableCollectors.toMap(
                            Map.Entry::getKey,
                            e -> new QualifiedAttributeID(alias, e.getValue().getAttribute())));
        }

        private ImmutableMap<Variable, QuotedID> createVariableAliases(ImmutableSet<Variable> variables) {
            AttributeAliasFactory aliasFactory = createAttributeAliasFactory();
            return variables.stream()
                    .collect(ImmutableCollectors.toMap(
                            Function.identity(),
                            v -> aliasFactory.createAttributeAlias(v.getName())));
        }

        protected AttributeAliasFactory createAttributeAliasFactory() {
            return new DefaultAttributeAliasFactory(idFactory);
        }

        protected String serializeDummyTable() {
            return "";
        }

        protected String serializeProjection(ImmutableSortedSet<Variable> projectedVariables, // only for ORDER
                                             ImmutableMap<Variable, QuotedID> variableAliases,
                                             ImmutableSubstitution<? extends ImmutableTerm> substitution,
                                             ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {

            if (projectedVariables.isEmpty())
                return "1 AS uselessVariable";

            return projectedVariables.stream()
                    .map(v -> sqlTermSerializer.serialize(
                            Optional.ofNullable((ImmutableTerm)substitution.get(v)).orElse(v),
                            columnIDs)
                            + " AS " + variableAliases.get(v).getSQLRendering())
                    .collect(Collectors.joining(", "));
        }

        protected String serializeGroupBy(ImmutableSet<Variable> groupByVariables,
                                          ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
            if (groupByVariables.isEmpty())
                return "";

            String variableString = groupByVariables.stream()
                    .map(v -> sqlTermSerializer.serialize(v, columnIDs))
                    .collect(Collectors.joining(", "));

            return String.format("GROUP BY %s\n", variableString);
        }

        protected String serializeOrderBy(ImmutableList<SQLOrderComparator> sortConditions,
                                          ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
            if (sortConditions.isEmpty())
                return "";

            String conditionString = sortConditions.stream()
                    .map(c -> sqlTermSerializer.serialize(c.getTerm(), columnIDs)
                            + (c.isAscending() ? " NULLS FIRST" : " DESC NULLS LAST"))
                    .collect(Collectors.joining(", "));

            return String.format("ORDER BY %s\n", conditionString);
        }

        /**
         * There is no standard for these three methods (may not work with many DB engines).
         */
        protected String serializeLimitOffset(long limit, long offset) {
            return String.format("LIMIT %d, %d", offset, limit);
        }

        protected String serializeLimit(long limit) {
            return String.format("LIMIT %d", limit);
        }

        protected String serializeOffset(long offset) {
            return String.format("OFFSET %d", offset);
        }


        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        private String serializeSlice(Optional<Long> limit, Optional<Long> offset) {
            if (!limit.isPresent() && !offset.isPresent())
                return "";

            if (limit.isPresent() && offset.isPresent())
                return serializeLimitOffset(limit.get(), offset.get());

            if (limit.isPresent())
                return serializeLimit(limit.get());

            return serializeOffset(offset.get());
        }


        @Override
        public QuerySerialization visit(SQLSerializedQuery sqlSerializedQuery) {
            RelationID alias = generateFreshViewAlias();
            String sql = String.format("(%s) %s",sqlSerializedQuery.getSQLString(), alias.getSQLRendering());
            return new QuerySerializationImpl(sql, attachRelationAlias(alias, sqlSerializedQuery.getColumnNames()));
        }

        @Override
        public QuerySerialization visit(SQLTable sqlTable) {
            RelationID alias = generateFreshViewAlias();
            RelationDefinition relation = sqlTable.getRelationDefinition();
            String relationRendering = relation.getAtomPredicate().getName();
            String sql = String.format("%s %s", relationRendering, alias.getSQLRendering());
            return new QuerySerializationImpl(sql, attachRelationAlias(alias, sqlTable.getArgumentMap().entrySet().stream()
                    .collect(ImmutableCollectors.toMap(
                            // Ground terms must have been already removed from atoms
                            e -> (Variable) e.getValue(),
                            e -> relation.getAttribute(e.getKey() + 1).getID()))));
        }

        @Override
        public QuerySerialization visit(SQLNaryJoinExpression sqlNaryJoinExpression) {
            ImmutableList<QuerySerialization> querySerializationList = sqlNaryJoinExpression.getJoinedExpressions().stream()
                    .map(this::getSQLSerializationForChild)
                    .collect(ImmutableCollectors.toList());

            String sql = querySerializationList.stream()
                    .map(QuerySerialization::getString)
                    .collect(Collectors.joining(", "));

            ImmutableMap<Variable, QualifiedAttributeID> columnIDs = querySerializationList.stream()
                    .flatMap(m -> m.getColumnIDs().entrySet().stream())
                    .collect(ImmutableCollectors.toMap());

            return new QuerySerializationImpl(sql, columnIDs);
        }

        @Override
        public QuerySerialization visit(SQLUnionExpression sqlUnionExpression) {
            ImmutableList<QuerySerialization> querySerializationList = sqlUnionExpression.getSubExpressions().stream()
                    .map(e -> e.acceptVisitor(this))
                    .collect(ImmutableCollectors.toList());

            RelationID alias = generateFreshViewAlias();
            String sql = String.format("(%s) %s", querySerializationList.stream()
                    .map(QuerySerialization::getString)
                    .collect(Collectors.joining("UNION ALL \n")), alias.getSQLRendering());

            return new QuerySerializationImpl(sql,
                    replaceRelationAlias(alias, querySerializationList.get(0).getColumnIDs()));
        }

        //this function is required in case at least one of the children is
        // SelectFromWhereWithModifiers expression
        private QuerySerialization getSQLSerializationForChild(SQLExpression expression) {
            if (expression instanceof SelectFromWhereWithModifiers) {
                QuerySerialization serialization = expression.acceptVisitor(this);
                RelationID alias = generateFreshViewAlias();
                String sql = String.format("(%s) %s", serialization.getString(), alias.getSQLRendering());
                return new QuerySerializationImpl(sql,
                        replaceRelationAlias(alias, serialization.getColumnIDs()));
            }
            return expression.acceptVisitor(this);
        }

        @Override
        public QuerySerialization visit(SQLInnerJoinExpression sqlInnerJoinExpression) {
            return visit(sqlInnerJoinExpression, "JOIN");
        }

        @Override
        public QuerySerialization visit(SQLLeftJoinExpression sqlLeftJoinExpression) {
            return visit(sqlLeftJoinExpression, "LEFT OUTER JOIN");
        }

        /**
         * NB: the systematic use of ON conditions for inner and left joins saves us from putting parentheses.
         *
         * Indeed since a join expression with a ON is always "CHILD_1 SOME_JOIN CHILD_2 ON COND",
         * the decomposition is unambiguous just following this pattern.
         *
         * For instance, "T1 LEFT JOIN T2 INNER JOIN T3 ON 1=1 ON 2=2"
         * is clearly equivalent to "T1 LEFT JOIN (T2 INNER JOIN T3)"
         * as the latest ON condition ("ON 2=2") can only be attached to the left join, which means that "T2 INNER JOIN T3 ON 1=1"
         * is the right child of the left join.
         *
         */
        protected QuerySerialization visit(BinaryJoinExpression binaryJoinExpression, String operatorString) {
            QuerySerialization left = getSQLSerializationForChild(binaryJoinExpression.getLeft());
            QuerySerialization right = getSQLSerializationForChild(binaryJoinExpression.getRight());

            ImmutableMap<Variable, QualifiedAttributeID> columnIDs = ImmutableList.of(left,right).stream()
                    .flatMap(m -> m.getColumnIDs().entrySet().stream())
                    .collect(ImmutableCollectors.toMap());

            String onString = binaryJoinExpression.getFilterCondition()
                    .map(e -> sqlTermSerializer.serialize(e, columnIDs))
                    .map(s -> String.format("ON %s ", s))
                    .orElse("ON 1 = 1 ");

            String sql = formatBinaryJoin(operatorString, left, right, onString);
            return new QuerySerializationImpl(sql, columnIDs);
        }

        protected String formatBinaryJoin(String operatorString, QuerySerialization left, QuerySerialization right, String onString) {
            return String.format("%s\n %s \n%s %s", left.getString(), operatorString, right.getString(), onString);
        }

        @Override
        public QuerySerialization visit(SQLOneTupleDummyQueryExpression sqlOneTupleDummyQueryExpression) {
            String fromString = serializeDummyTable();
            String sqlSubString = String.format("(SELECT 1 %s) tdummy", fromString);
            return new QuerySerializationImpl(sqlSubString, ImmutableMap.of());
        }
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