package it.unibz.inf.ontop.iq.node.normalization.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import it.unibz.inf.ontop.exception.MinorOntopInternalBugException;
import it.unibz.inf.ontop.injection.CoreSingletons;
import it.unibz.inf.ontop.injection.IntermediateQueryFactory;
import it.unibz.inf.ontop.iq.IQProperties;
import it.unibz.inf.ontop.iq.IQTree;
import it.unibz.inf.ontop.iq.node.ConstructionNode;
import it.unibz.inf.ontop.iq.node.VariableNullability;
import it.unibz.inf.ontop.model.term.ImmutableFunctionalTerm;
import it.unibz.inf.ontop.model.term.ImmutableTerm;
import it.unibz.inf.ontop.model.term.NonFunctionalTerm;
import it.unibz.inf.ontop.model.term.Variable;
import it.unibz.inf.ontop.substitution.ImmutableSubstitution;
import it.unibz.inf.ontop.substitution.SubstitutionFactory;
import it.unibz.inf.ontop.utils.ImmutableCollectors;
import it.unibz.inf.ontop.utils.VariableGenerator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public abstract class InjectiveBindingLiftState {

    // The parent (closest ancestor) is first
    protected final ImmutableList<ConstructionNode> ancestors;
    // First descendent tree not starting with a construction node
    protected final IQTree grandChildTree;
    @Nullable
    protected final ConstructionNode childConstructionNode;
    protected final VariableGenerator variableGenerator;
    protected final CoreSingletons coreSingletons;

    /**
     * Initial state
     */
    protected InjectiveBindingLiftState(@Nonnull ConstructionNode childConstructionNode, IQTree grandChildTree,
                                     VariableGenerator variableGenerator, CoreSingletons coreSingletons) {
        this.coreSingletons = coreSingletons;
        this.ancestors = ImmutableList.of();
        this.grandChildTree = grandChildTree;
        this.childConstructionNode = childConstructionNode;
        this.variableGenerator = variableGenerator;
    }

    protected InjectiveBindingLiftState(ImmutableList<ConstructionNode> ancestors, IQTree grandChildTree,
                                      VariableGenerator variableGenerator, CoreSingletons coreSingletons) {
        this.ancestors = ancestors;
        this.grandChildTree = grandChildTree;
        this.coreSingletons = coreSingletons;
        this.childConstructionNode = null;
        this.variableGenerator = variableGenerator;
    }

    protected InjectiveBindingLiftState(ImmutableList<ConstructionNode> ancestors, IQTree grandChildTree,
                                      VariableGenerator variableGenerator,
                                      @Nonnull ConstructionNode childConstructionNode, CoreSingletons coreSingletons) {
        this.ancestors = ancestors;
        this.grandChildTree = grandChildTree;
        this.childConstructionNode = childConstructionNode;
        this.variableGenerator = variableGenerator;
        this.coreSingletons = coreSingletons;
    }

    public InjectiveBindingLiftState liftBindings() {
        if (childConstructionNode == null)
            return this;

        ImmutableSubstitution<ImmutableTerm> childSubstitution = childConstructionNode.getSubstitution();
        if (childSubstitution.isEmpty())
            return this;

        VariableNullability grandChildVariableNullability = grandChildTree.getVariableNullability();

        ImmutableSet<Variable> nonFreeVariables = childConstructionNode.getVariables();

        ImmutableMap<Variable, Optional<ImmutableFunctionalTerm.FunctionalTermDecomposition>> injectivityDecompositionMap =
                childSubstitution.getImmutableMap().entrySet().stream()
                        .filter(e -> e.getValue() instanceof ImmutableFunctionalTerm)
                        .collect(ImmutableCollectors.toMap(
                                Map.Entry::getKey,
                                e -> ((ImmutableFunctionalTerm) e.getValue())
                                        // Analyzes injectivity
                                        .analyzeInjectivity(nonFreeVariables, grandChildVariableNullability,
                                                variableGenerator)));

        ImmutableMap<Variable, ImmutableTerm> liftedSubstitutionMap = Stream.concat(
                // All variables and constants
                childSubstitution.getImmutableMap().entrySet().stream()
                        .filter(e -> e.getValue() instanceof NonFunctionalTerm),
                // (Possibly decomposed) injective functional terms
                injectivityDecompositionMap.entrySet().stream()
                        .filter(e -> e.getValue().isPresent())
                        .map(e -> Maps.immutableEntry(e.getKey(),
                                (ImmutableTerm) e.getValue().get().getLiftableTerm())))
                .collect(ImmutableCollectors.toMap());

        SubstitutionFactory substitutionFactory = coreSingletons.getSubstitutionFactory();
        IntermediateQueryFactory iqFactory = coreSingletons.getIQFactory();

        Optional<ConstructionNode> liftedConstructionNode = Optional.of(liftedSubstitutionMap)
                .filter(m -> !m.isEmpty())
                .map(substitutionFactory::getSubstitution)
                .map(s -> iqFactory.createConstructionNode(childConstructionNode.getVariables(), s));

        ImmutableSet<Variable> newChildVariables = liftedConstructionNode
                .map(ConstructionNode::getChildVariables)
                .orElseGet(childConstructionNode::getVariables);

        ImmutableMap<Variable, ImmutableTerm> newChildSubstitutionMap =
                injectivityDecompositionMap.entrySet().stream()
                        .flatMap(e -> e.getValue()
                                // Sub-term substitution entries from injectivity decompositions
                            .map(d -> d.getSubTermSubstitutionMap()
                                    .map(s -> s.entrySet().stream()
                                            .map(subE -> (Map.Entry<Variable, ImmutableTerm>)(Map.Entry<Variable, ?>) subE))
                                    .orElseGet(Stream::empty))
                                // Non-decomposable entries
                            .orElseGet(() -> Stream.of(Maps.immutableEntry(
                                    e.getKey(),
                                    childSubstitution.get(e.getKey())))))
                        .collect(ImmutableCollectors.toMap());

        Optional<ConstructionNode> newChildConstructionNode = Optional.of(newChildSubstitutionMap)
                .filter(m -> !m.isEmpty())
                .map(substitutionFactory::getSubstitution)
                .map(s -> iqFactory.createConstructionNode(newChildVariables, s))
                .map(Optional::of)
                .orElseGet(() -> newChildVariables.equals(grandChildTree.getVariables())
                        ? Optional.empty()
                        : Optional.of(iqFactory.createConstructionNode(newChildVariables)));

        // Nothing lifted
        if (newChildConstructionNode
                .filter(n -> n.isEquivalentTo(childConstructionNode))
                .isPresent()) {
            if (liftedConstructionNode.isPresent())
                throw new MinorOntopInternalBugException("Unexpected lifted construction node");
            return this;
        }

        ImmutableList<ConstructionNode> newAncestors = liftedConstructionNode
                .map(n -> Stream.concat(ancestors.stream(), Stream.of(n))
                        .collect(ImmutableCollectors.toList()))
                .orElseThrow(() -> new MinorOntopInternalBugException("A lifted construction node was expected"));

        return newChildConstructionNode
                .map(c -> newState(newAncestors, grandChildTree, c))
                .orElseGet(() -> newState(newAncestors, grandChildTree));
    }

    protected abstract InjectiveBindingLiftState newState(ImmutableList<ConstructionNode> newAncestors, IQTree grandChildTree);

    protected abstract InjectiveBindingLiftState newState(ImmutableList<ConstructionNode> newAncestors, IQTree grandChildTree,
                                                          ConstructionNode childConstructionNode);

    public abstract IQTree createNormalizedTree(IQProperties currentIQProperties);
}
