package it.unibz.inf.ontop.injection.impl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import it.unibz.inf.ontop.injection.CoreSingletons;
import it.unibz.inf.ontop.injection.OntopOptimizationSettings;
import it.unibz.inf.ontop.injection.OptimizationSingletons;
import it.unibz.inf.ontop.injection.OptimizerFactory;
import it.unibz.inf.ontop.iq.tools.UnionBasedQueryMerger;
import it.unibz.inf.ontop.iq.visitor.RequiredDataAtomExtractor;

@Singleton
public class OptimizationSingletonsImpl implements OptimizationSingletons {

    private final OptimizerFactory optimizerFactory;
    private final CoreSingletons coreSingletons;
    private final UnionBasedQueryMerger unionBasedQueryMerger;
    private final RequiredDataAtomExtractor requiredDataAtomExtractor;
    private final OntopOptimizationSettings settings;

    @Inject
    protected OptimizationSingletonsImpl(OptimizerFactory optimizerFactory, CoreSingletons coreSingletons,
                                         UnionBasedQueryMerger unionBasedQueryMerger, RequiredDataAtomExtractor requiredDataAtomExtractor,
                                         OntopOptimizationSettings settings) {
        this.optimizerFactory = optimizerFactory;
        this.coreSingletons = coreSingletons;
        this.unionBasedQueryMerger = unionBasedQueryMerger;
        this.requiredDataAtomExtractor = requiredDataAtomExtractor;
        this.settings = settings;
    }

    @Override
    public CoreSingletons getCoreSingletons() {
        return coreSingletons;
    }

    @Override
    public OptimizerFactory getOptimizerFactory() {
        return optimizerFactory;
    }

    @Override
    public UnionBasedQueryMerger getUnionBasedQueryMerger() {
        return unionBasedQueryMerger;
    }

    @Override
    public OntopOptimizationSettings getSettings() {
        return settings;
    }

    @Override
    public RequiredDataAtomExtractor getRequiredDataAtomExtractor() {
        return requiredDataAtomExtractor;
    }
}
