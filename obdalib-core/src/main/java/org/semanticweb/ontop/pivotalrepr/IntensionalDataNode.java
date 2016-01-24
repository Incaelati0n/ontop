package org.semanticweb.ontop.pivotalrepr;

import org.semanticweb.ontop.model.DataAtom;
import org.semanticweb.ontop.model.ImmutableSubstitution;
import org.semanticweb.ontop.model.VariableOrGroundTerm;

/**
 * TODO: explain
 * TODO: find a better name
 */
public interface IntensionalDataNode extends DataNode {

    @Override
    IntensionalDataNode clone();

    @Override
    IntensionalDataNode acceptNodeTransformer(HomogeneousQueryNodeTransformer transformer) throws QueryNodeTransformationException;

    @Override
    SubstitutionResults<IntensionalDataNode> applyAscendentSubstitution(
            ImmutableSubstitution<? extends VariableOrGroundTerm> substitution,
            QueryNode descendantNode, IntermediateQuery query);

    @Override
    SubstitutionResults<IntensionalDataNode> applyDescendentSubstitution(
            ImmutableSubstitution<? extends VariableOrGroundTerm> substitution);

    @Override
    IntensionalDataNode newAtom(DataAtom newAtom);
}