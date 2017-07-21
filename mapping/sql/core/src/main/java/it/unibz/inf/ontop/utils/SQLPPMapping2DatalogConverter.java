package it.unibz.inf.ontop.utils;

/*
 * #%L
 * ontop-obdalib-core
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.datalog.CQIE;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.exception.InvalidMappingSourceQueriesException;
import it.unibz.inf.ontop.model.*;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.pp.PPMappingAssertionProvenance;
import it.unibz.inf.ontop.pp.PPTriplesMapProvenance;
import it.unibz.inf.ontop.mapping.pp.SQLPPTriplesMap;

import java.util.*;

import it.unibz.inf.ontop.sql.parser.RAExpression;
import it.unibz.inf.ontop.sql.parser.SelectQueryAttributeExtractor;
import it.unibz.inf.ontop.sql.parser.SelectQueryParser;
import it.unibz.inf.ontop.sql.parser.exceptions.InvalidSelectQueryException;
import it.unibz.inf.ontop.sql.parser.exceptions.UnsupportedSelectQueryException;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static it.unibz.inf.ontop.model.OntopModelSingletons.DATALOG_FACTORY;
import static it.unibz.inf.ontop.model.OntopModelSingletons.DATA_FACTORY;


public class SQLPPMapping2DatalogConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLPPMapping2DatalogConverter.class);

    /**
     * returns a Datalog representation of the mappings
     */
    public static ImmutableList<CQIE> constructDatalogProgram(Collection<SQLPPTriplesMap> triplesMaps,
                                                              DBMetadata metadata) throws InvalidMappingSourceQueriesException {
        return ImmutableList.copyOf(convert(triplesMaps, metadata).keySet());
    }

    public static ImmutableMap<CQIE, PPMappingAssertionProvenance> convert(Collection<SQLPPTriplesMap> triplesMaps,
                                                                     DBMetadata metadata0) throws InvalidMappingSourceQueriesException {
        Map<CQIE, PPMappingAssertionProvenance> mutableMap = new HashMap<>();

        RDBMetadata metadata = (RDBMetadata)metadata0;

        List<String> errorMessages = new ArrayList<>();

        QuotedIDFactory idfac = metadata.getQuotedIDFactory();

        for (SQLPPTriplesMap mappingAxiom : triplesMaps) {
            try {
                OBDASQLQuery sourceQuery = mappingAxiom.getSourceQuery();

                SelectQueryParser sqp = new SelectQueryParser(metadata);
                List<Function> body;
                ImmutableMap<QualifiedAttributeID, Variable> lookupTable;

                try {
                    RAExpression re = sqp.parse(sourceQuery.toString());
                    lookupTable = re.getAttributes();

                    body = new ArrayList<>(re.getDataAtoms().size() + re.getFilterAtoms().size());
                    body.addAll(re.getDataAtoms());
                    body.addAll(re.getFilterAtoms());
                }
                catch (UnsupportedSelectQueryException e) {
                    // WRAP UP
                    //ImmutableSet<QuotedID> variableNames = mappingAxiom.getTargetQuery().stream()
                    //        .map(f -> collectVariableNames(idfac, f))
                    //        .reduce((s1, s2) -> ImmutableSet.<QuotedID>builder().addAll(s1).addAll(s2).build())
                    //        .get();
                    ImmutableList<QuotedID> variableNames =
                            new SelectQueryAttributeExtractor(metadata).extract(sourceQuery.toString());

                    ParserViewDefinition view = metadata.createParserView(sourceQuery.toString());
                    // TODO: clean up
                    boolean needsCreating = view.getAttributes().isEmpty();
                    ImmutableMap.Builder<QualifiedAttributeID, Variable> builder = ImmutableMap.builder();
                    List<Term> arguments = new ArrayList<>(variableNames.size());
                    variableNames.forEach(id -> {
                        QualifiedAttributeID qId = new QualifiedAttributeID(null, id);
                        if (needsCreating)
                            view.addAttribute(qId);
                        Variable var = DATA_FACTORY.getVariable(id.getName());
                        builder.put(qId, var);
                        arguments.add(var);
                    });

                    lookupTable = builder.build();

                    body = new ArrayList<>(1);
                    body.add(DATA_FACTORY.getFunction(Relation2Predicate.createPredicateFromRelation(view), arguments));
                }

                for (ImmutableFunctionalTerm atom : mappingAxiom.getTargetAtoms()) {
                    PPMappingAssertionProvenance provenance = mappingAxiom.getMappingAssertionProvenance(atom);
                    Function head = renameVariables(atom, lookupTable, idfac);
                    CQIE rule = DATALOG_FACTORY.getCQIE(head, body);

                    if (mutableMap.containsKey(rule)) {
                        LOGGER.warn("Redundant triples maps: \n" + provenance + "\n and \n" + mutableMap.get(rule));
                    }
                    else {
                        mutableMap.put(rule, provenance);
                    }
                }
            }
            catch (InvalidSelectQueryException e) {
                errorMessages.add("Error: " + e.getMessage()
                        + " in the source query of triplesMap \n["
                        +  mappingAxiom.getTriplesMapProvenance().getProvenanceInfo() + "]");
            }
        }

        if (!errorMessages.isEmpty())
            throw new InvalidMappingSourceQueriesException(Joiner.on("\n\n").join(errorMessages));

        return ImmutableMap.copyOf(mutableMap);
    }


    /**
     * Returns a new function by renaming variables occurring in the {@code function}
     *  according to the {@code attributes} lookup table
     */
    private static Function renameVariables(Function function, ImmutableMap<QualifiedAttributeID, Variable> attributes, QuotedIDFactory idfac) {
        List<Term> terms = function.getTerms();
        List<Term> newTerms = new ArrayList<>(terms.size());
        for (Term t : terms)
            newTerms.add(renameTermVariables(t, attributes, idfac));

        return DATA_FACTORY.getFunction(function.getFunctionSymbol(), newTerms);
    }

    /**
     * Returns a new term by renaming variables occurring in the {@code term}
     *  according to the {@code attributes} lookup table
     */
    private static Term renameTermVariables(Term term, ImmutableMap<QualifiedAttributeID, Variable> attributes, QuotedIDFactory idfac) {

        if (term instanceof Variable) {
            Variable var = (Variable) term;
            String varName = var.getName();
            // TODO: remove this code
            // chop off the qualifying table name
            if (varName.contains("."))
                varName = varName.substring(varName.indexOf(".") + 1);
            QuotedID attribute = idfac.createAttributeID(varName);
            Variable newVar = attributes.get(new QualifiedAttributeID(null, attribute));

            if (newVar == null) {
                QuotedID quotedAttribute = QuotedID.createIdFromDatabaseRecord(idfac, varName);
                newVar = attributes.get(new QualifiedAttributeID(null, quotedAttribute));

                if (newVar == null)
                    throw new IllegalArgumentException("Column " + attribute + " ( " + var.getName() + " ) not found in " + attributes);
            }

            return newVar;
        }
        else if (term instanceof Function)
            return renameVariables((Function) term, attributes, idfac);

        else if (term instanceof Constant)
            return term.clone();

        throw new RuntimeException("Unknown term type: " + term);
    }
}
