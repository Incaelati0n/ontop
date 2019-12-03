package it.unibz.inf.ontop.spec.mapping.transformer.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import it.unibz.inf.ontop.exception.MinorOntopInternalBugException;
import it.unibz.inf.ontop.injection.CoreSingletons;
import it.unibz.inf.ontop.injection.IntermediateQueryFactory;
import it.unibz.inf.ontop.injection.ProvenanceMappingFactory;
import it.unibz.inf.ontop.iq.IQ;
import it.unibz.inf.ontop.iq.IQTree;
import it.unibz.inf.ontop.iq.node.*;
import it.unibz.inf.ontop.iq.transform.IQTreeTransformer;
import it.unibz.inf.ontop.iq.transform.impl.DefaultRecursiveIQTreeVisitingTransformer;
import it.unibz.inf.ontop.iq.type.UniqueTermTypeExtractor;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.term.functionsymbol.FunctionSymbol;
import it.unibz.inf.ontop.model.term.functionsymbol.NotYetTypedEqualityFunctionSymbol;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.model.type.TermType;
import it.unibz.inf.ontop.spec.mapping.MappingWithProvenance;
import it.unibz.inf.ontop.spec.mapping.pp.PPMappingAssertionProvenance;
import it.unibz.inf.ontop.spec.mapping.transformer.MappingEqualityTransformer;
import it.unibz.inf.ontop.substitution.ImmutableSubstitution;
import it.unibz.inf.ontop.substitution.SubstitutionFactory;
import it.unibz.inf.ontop.utils.ImmutableCollectors;

import javax.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class MappingEqualityTransformerImpl implements MappingEqualityTransformer {

    private final ProvenanceMappingFactory mappingFactory;
    private final IQTreeTransformer expressionTransformer;
    private final IntermediateQueryFactory iqFactory;

    @Inject
    protected MappingEqualityTransformerImpl(ProvenanceMappingFactory mappingFactory,
                                             UniqueTermTypeExtractor typeExtractor, CoreSingletons coreSingletons) {
        this.mappingFactory = mappingFactory;
        this.expressionTransformer = new ExpressionTransformer(typeExtractor, coreSingletons);
        this.iqFactory = coreSingletons.getIQFactory();
    }

    @Override
    public MappingWithProvenance transform(MappingWithProvenance mapping) {
        ImmutableMap<IQ, PPMappingAssertionProvenance> newProvenanceMap = mapping.getProvenanceMap().entrySet().stream()
                .collect(ImmutableCollectors.toMap(
                        e -> transformMappingAssertion(e.getKey()),
                        Map.Entry::getValue));
        return mappingFactory.create(newProvenanceMap, mapping.getMetadata());
    }

    private IQ transformMappingAssertion(IQ mappingAssertion) {
        IQTree initialTree = mappingAssertion.getTree();
        IQTree newTree = expressionTransformer.transform(initialTree);
        return (newTree.equals(initialTree))
                ? mappingAssertion
                : iqFactory.createIQ(mappingAssertion.getProjectionAtom(), newTree);
    }


    protected static class ExpressionTransformer extends DefaultRecursiveIQTreeVisitingTransformer {

        protected final UniqueTermTypeExtractor typeExtractor;
        protected final TermFactory termFactory;
        private final SubstitutionFactory substitutionFactory;

        protected ExpressionTransformer(UniqueTermTypeExtractor typeExtractor, CoreSingletons coreSingletons) {
            super(coreSingletons);
            this.typeExtractor = typeExtractor;
            this.termFactory = coreSingletons.getTermFactory();
            this.substitutionFactory = coreSingletons.getSubstitutionFactory();
        }

        @Override
        public IQTree transformConstruction(IQTree tree, ConstructionNode rootNode, IQTree child) {
            IQTree newChild = transform(child);

            ImmutableSubstitution<ImmutableTerm> initialSubstitution = rootNode.getSubstitution();

            ImmutableSubstitution<ImmutableTerm> newSubstitution = substitutionFactory.getSubstitution(
                    initialSubstitution.getImmutableMap().entrySet().stream()
                            .collect(ImmutableCollectors.toMap(
                                    Map.Entry::getKey,
                                    e -> transformTerm(e.getValue(), child))));

            return (newChild.equals(child) && newSubstitution.equals(initialSubstitution))
                    ? tree
                    : iqFactory.createUnaryIQTree(
                            iqFactory.createConstructionNode(rootNode.getVariables(), newSubstitution),
                            newChild);
        }

        @Override
        public IQTree transformAggregation(IQTree tree, AggregationNode rootNode, IQTree child) {
            IQTree newChild = transform(child);

            ImmutableSubstitution<ImmutableFunctionalTerm> initialSubstitution = rootNode.getSubstitution();

            ImmutableSubstitution<ImmutableFunctionalTerm> newSubstitution = substitutionFactory.getSubstitution(
                    initialSubstitution.getImmutableMap().entrySet().stream()
                            .collect(ImmutableCollectors.toMap(
                                    Map.Entry::getKey,
                                    e -> transformFunctionalTerm(e.getValue(), child))));

            return (newChild.equals(child) && newSubstitution.equals(initialSubstitution))
                    ? tree
                    : iqFactory.createUnaryIQTree(
                        iqFactory.createAggregationNode(rootNode.getGroupingVariables(), newSubstitution),
                        newChild);
        }

        @Override
        public IQTree transformFilter(IQTree tree, FilterNode rootNode, IQTree child) {
            IQTree newChild = transform(child);
            ImmutableExpression initialExpression = rootNode.getFilterCondition();
            ImmutableExpression newExpression = transformExpression(initialExpression, tree);

            FilterNode newFilterNode = newExpression.equals(initialExpression)
                    ? rootNode
                    : rootNode.changeFilterCondition(newExpression);

            return (newFilterNode.equals(rootNode) && newChild.equals(child))
                    ? tree
                    : iqFactory.createUnaryIQTree(newFilterNode, newChild);
        }

        @Override
        public IQTree transformOrderBy(IQTree tree, OrderByNode rootNode, IQTree child) {
            IQTree newChild = transform(child);

            ImmutableList<OrderByNode.OrderComparator> initialComparators = rootNode.getComparators();

            ImmutableList<OrderByNode.OrderComparator> newComparators = initialComparators.stream()
                    .map(c -> iqFactory.createOrderComparator(
                            transformNonGroundTerm(c.getTerm(), tree),
                            c.isAscending()))
                    .collect(ImmutableCollectors.toList());

            return (newComparators.equals(initialComparators) && newChild.equals(child))
                    ? tree
                    : iqFactory.createUnaryIQTree(
                            iqFactory.createOrderByNode(newComparators),
                            newChild);
        }

        @Override
        public IQTree transformLeftJoin(IQTree tree, LeftJoinNode rootNode, IQTree leftChild, IQTree rightChild) {
            IQTree newLeftChild = transform(leftChild);
            IQTree newRightChild = transform(rightChild);
            Optional<ImmutableExpression> initialExpression = rootNode.getOptionalFilterCondition();
            Optional<ImmutableExpression> newExpression = initialExpression
                    .map(e -> transformExpression(e, tree));

            LeftJoinNode newLeftJoinNode = newExpression.equals(initialExpression)
                    ? rootNode
                    : rootNode.changeOptionalFilterCondition(newExpression);

            return (newLeftJoinNode.equals(rootNode) && newLeftChild.equals(leftChild) && newRightChild.equals(rightChild))
                    ? tree
                    : iqFactory.createBinaryNonCommutativeIQTree(newLeftJoinNode, newLeftChild, newRightChild);
        }

        @Override
        public IQTree transformInnerJoin(IQTree tree, InnerJoinNode rootNode, ImmutableList<IQTree> children) {
            ImmutableList<IQTree> newChildren = children.stream()
                    .map(this::transform)
                    .collect(ImmutableCollectors.toList());

            Optional<ImmutableExpression> initialExpression = rootNode.getOptionalFilterCondition();
            Optional<ImmutableExpression> newExpression = initialExpression
                    .map(e -> transformExpression(e, tree));

            InnerJoinNode newJoinNode = newExpression.equals(initialExpression)
                    ? rootNode
                    : rootNode.changeOptionalFilterCondition(newExpression);

            return (newJoinNode.equals(rootNode) && newChildren.equals(children))
                    ? tree
                    : iqFactory.createNaryIQTree(newJoinNode, newChildren);
        }

        protected ImmutableTerm transformTerm(ImmutableTerm term, IQTree tree) {
            return (term instanceof ImmutableFunctionalTerm)
                    ? transformFunctionalTerm((ImmutableFunctionalTerm)term, tree)
                    : term;
        }

        protected NonGroundTerm transformNonGroundTerm(NonGroundTerm term, IQTree tree) {
            return (term instanceof ImmutableFunctionalTerm)
                    ? (NonGroundTerm) transformFunctionalTerm((ImmutableFunctionalTerm)term, tree)
                    : term;
        }

        protected ImmutableExpression transformExpression(ImmutableExpression expression, IQTree tree) {
            return (ImmutableExpression) transformFunctionalTerm(expression, tree);
        }

        /**
         * Recursive
         */
        protected ImmutableFunctionalTerm transformFunctionalTerm(ImmutableFunctionalTerm functionalTerm, IQTree tree) {
            ImmutableList<? extends ImmutableTerm> initialTerms = functionalTerm.getTerms();
            ImmutableList<ImmutableTerm> newTerms = initialTerms.stream()
                    .map(t -> (t instanceof ImmutableFunctionalTerm)
                            // Recursive
                            ? transformFunctionalTerm((ImmutableFunctionalTerm) t, tree)
                            : t)
                    .collect(ImmutableCollectors.toList());

            FunctionSymbol functionSymbol = functionalTerm.getFunctionSymbol();

            if (functionSymbol instanceof NotYetTypedEqualityFunctionSymbol) {
                return transformEquality(newTerms, tree);
            }
            else
                return newTerms.equals(initialTerms)
                        ? functionalTerm
                        : termFactory.getImmutableFunctionalTerm(functionSymbol, newTerms);
        }

        /**
         * NB: It tries to reduce equalities into strict equalities.
         * 
         * Essential for integers and strings as these kinds types are often used to build IRIs.
         */
        protected ImmutableExpression transformEquality(ImmutableList<ImmutableTerm> newTerms, IQTree tree) {
            if (newTerms.size() != 2)
                throw new MinorOntopInternalBugException("Was expecting the not yet typed equalities to be binary");

            ImmutableTerm term1 = newTerms.get(0);
            ImmutableTerm term2 = newTerms.get(1);

            ImmutableList<Optional<TermType>> extractedTypes = newTerms.stream()
                    .map(t -> typeExtractor.extractUniqueTermType(t, tree))
                    .collect(ImmutableCollectors.toList());

            if (extractedTypes.stream()
                    .allMatch(type -> type
                            .filter(t -> t instanceof DBTermType)
                            .isPresent())) {
                ImmutableList<DBTermType> types = extractedTypes.stream()
                        .map(Optional::get)
                        .map(t -> (DBTermType) t)
                        .collect(ImmutableCollectors.toList());

                DBTermType type1 = types.get(0);
                DBTermType type2 = types.get(1);

                return type1.equals(type2)
                        ? transformSameTypeEquality(type1, term1, term2, tree)
                        : transformDifferentTypesEquality(type1, type2, term1, term2);
            }
            else
                return termFactory.getDBNonStrictDefaultEquality(term1, term2);
        }

        private ImmutableExpression transformSameTypeEquality(DBTermType type, ImmutableTerm term1, ImmutableTerm term2,
                                                              IQTree tree) {
            if (type.areEqualitiesStrict())
                return termFactory.getStrictEquality(term1, term2);

            if (areIndependentFromConstants(term1, term2, tree) && type.areEqualitiesBetweenTwoDBAttributesStrict()) {
                return termFactory.getStrictEquality(term1, term2);
            }

            /*
             * Tries to reuse an existing typed non-strict equality
             */
            switch (type.getCategory()) {
                case DECIMAL:
                case FLOAT_DOUBLE:
                    return termFactory.getDBNonStrictNumericEquality(term1, term2);
                case DATETIME:
                    return termFactory.getDBNonStrictDatetimeEquality(term1, term2);
                default:
                    return termFactory.getDBNonStrictDefaultEquality(term1, term2);
            }
        }

        protected ImmutableExpression transformDifferentTypesEquality(DBTermType type1, DBTermType type2,
                                                                      ImmutableTerm term1, ImmutableTerm term2) {
            /*
             * If not type declares that the equality cannot be reduced to a strict equality
             */
            if (areCompatibleForStrictEq(type1, type2)) {
                return termFactory.getStrictEquality(term1, term2);
            }

            /*
             * Tries to reuse an existing typed non-strict equality
             */
            DBTermType.Category category1 = type1.getCategory();
            DBTermType.Category category2 = type2.getCategory();

            switch (category1) {
                case STRING:
                case INTEGER:
                    switch (category2) {
                        case DECIMAL:
                        case FLOAT_DOUBLE:
                            return termFactory.getDBNonStrictNumericEquality(term1, term2);
                        default:
                            break;
                    }
                    break;
                case DECIMAL:
                case FLOAT_DOUBLE:
                    switch (category2) {
                        case INTEGER:
                        case DECIMAL:
                        case FLOAT_DOUBLE:
                            return termFactory.getDBNonStrictNumericEquality(term1, term2);
                        default:
                            break;
                    }
                    break;
                case DATETIME:
                    if (category2 == DBTermType.Category.DATETIME) {
                        return termFactory.getDBNonStrictDatetimeEquality(term1, term2);
                    }
                    break;
                case BOOLEAN:
                case OTHER:
                default:
                    break;
            }
            // By default
            return termFactory.getDBNonStrictDefaultEquality(term1, term2);
        }


        /*
         * TODO: make it robust so make sure the term does not depend on a constant introduced in the mapping.
         *
         * Constants in the mapping are indeed uncontrolled and may have a different lexical value
         * that the ones returned by the DB, which would make the test fail.
         */
        protected boolean areIndependentFromConstants(ImmutableTerm term1, ImmutableTerm term2, IQTree tree) {
            return !((term1 instanceof DBConstant) || (term2 instanceof DBConstant));
        }

        private boolean areCompatibleForStrictEq(DBTermType type1, DBTermType type2) {
            return Stream.of(type1.areEqualitiesStrict(type2), type2.areEqualitiesStrict(type1))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .reduce((b1, b2) -> b1 && b2)
                    .orElse(false);
        }
    }

}
