package it.unibz.inf.ontop.spec.mapping.parser.impl;

/*
 * #%L
 * ontop-obdalib-sesame
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

import com.google.common.collect.ImmutableList;
import eu.optique.r2rml.api.binding.rdf4j.RDF4JR2RMLMappingManager;
import eu.optique.r2rml.api.model.*;
import eu.optique.r2rml.api.model.impl.InvalidR2RMLMappingException;
import it.unibz.inf.ontop.exception.MinorOntopInternalBugException;
import it.unibz.inf.ontop.injection.OntopMappingSQLSettings;
import it.unibz.inf.ontop.injection.OntopMappingSettings;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.type.TypeFactory;
import it.unibz.inf.ontop.model.vocabulary.RDFS;
import it.unibz.inf.ontop.model.vocabulary.XSD;
import org.apache.commons.rdf.api.*;
import org.eclipse.rdf4j.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class R2RMLParser {

	List<NonVariableTerm> classPredicates;
	List<Resource> joinPredObjNodes;


    RDF4JR2RMLMappingManager mapManager;
	Logger logger = LoggerFactory.getLogger(R2RMLParser.class);
	private final TermFactory termFactory;
	private final TypeFactory typeFactory;
	private final OntopMappingSQLSettings settings;
	private final RDF rdfFactory;

	/**
	 * empty constructor
	 * @param termFactory
	 * @param typeFactory
	 */
	public R2RMLParser(TermFactory termFactory, TypeFactory typeFactory, RDF rdfFactory,
					   OntopMappingSQLSettings settings) {
		this.termFactory = termFactory;
		this.typeFactory = typeFactory;
		this.settings = settings;
		mapManager = RDF4JR2RMLMappingManager.getInstance();
		classPredicates = new ArrayList<>();
		joinPredObjNodes = new ArrayList<>();
		this.rdfFactory = rdfFactory;
	}

	/**
	 * method to get the TriplesMaps from the given Graph
	 * @param myGraph - the Graph to process
	 * @return Collection<TriplesMap> - the collection of mappings
	 */
	public Collection<TriplesMap> getMappingNodes(Graph myGraph) throws InvalidR2RMLMappingException {
		return mapManager.importMappings(myGraph);
	}

	/**
	 * Get SQL query of the TriplesMap
	 * @param tm
	 * @return
	 */
	public String getSQLQuery(TriplesMap tm) {
		return tm.getLogicalTable().getSQLQuery();
	}

	/**
	 * Get classes
     * They can be retrieved only once, after retrieving everything is cleared.
	 * @return
	 */
	public List<NonVariableTerm> getClassPredicates() {
		List<NonVariableTerm> classes = new ArrayList<>();
		for (NonVariableTerm p : classPredicates)
			classes.add(p);
		classPredicates.clear();
		return classes;
	}

	/**
	 * Get predicates
	 * @param tm
	 * @return
	 */
	public Set<BlankNodeOrIRI> getPredicateObjects(TriplesMap tm) {
		Set<BlankNodeOrIRI> predobjs = new HashSet<>();
		for (PredicateObjectMap pobj : tm.getPredicateObjectMaps()) {
			for (PredicateMap pm : pobj.getPredicateMaps()) {
				BlankNodeOrIRI r = pm.getNode();
				predobjs.add(r);
			}
		}
		return predobjs;
	}

	public ImmutableTerm getSubjectAtom(TriplesMap tm) throws Exception {
		return getSubjectAtom(tm, "");
	}

	/**
	 * Get subject
	 *
	 * @param tm
	 * @param joinCond
	 * @return
	 * @throws Exception
	 */
	public ImmutableTerm getSubjectAtom(TriplesMap tm, String joinCond) throws Exception {
		ImmutableTerm subjectAtom = null;
		String subj = "";
		classPredicates.clear();

		// SUBJECT
		SubjectMap sMap = tm.getSubjectMap();

		// process template declaration
		IRI termType = sMap.getTermType();

		// WORKAROUND for:
		// SubjectMap.getTemplateString() throws NullPointerException when
		// template == null
		//
		Template template = sMap.getTemplate();
		if (template == null) {
			subj = null;
		} else {
			subj = sMap.getTemplateString();
		}

		if (subj != null) {
			// create uri("...",var)
			subjectAtom = getTermTypeAtom(subj, termType, joinCond);
		}

		// process column declaration
		subj = sMap.getColumn();
		if (subj != null) {
			if(template == null && (termType.equals(R2RMLVocabulary.iri))){

				subjectAtom = termFactory.getRDFFunctionalTerm(
						termFactory.getPartiallyDefinedToStringCast(termFactory.getVariable(subj)),
						termFactory.getRDFTermTypeConstant(typeFactory.getIRITermType()));

			}
			else {
				// create uri("...",var)
				subjectAtom = getTermTypeAtom(subj, termType, joinCond);
			}
		}

		// process constant declaration
        // TODO(xiao): toString() is suspicious
        RDFTerm subjConstant = sMap.getConstant();
		if (subjConstant != null) {
			// create uri("...",var)
            subj = subjConstant.toString();
			subjectAtom = getURIFunction(subj, joinCond);
		}


		// process class declaration
		List<IRI> classes = sMap.getClasses();
		for (IRI o : classes) {
            classPredicates.add(termFactory.getConstantIRI(o));
		}

		if (subjectAtom == null)
			throw new Exception("Error in parsing the subjectMap in node "
					+ tm.toString());

		return subjectAtom;

	}

	/**
	 * Get body predicates with templates
	 * @param pom
	 * @return
	 * @throws Exception
	 */
	public List<NonVariableTerm> getBodyURIPredicates(PredicateObjectMap pom) {
		List<NonVariableTerm> predicateAtoms = new ArrayList<>();

		// process PREDICATEMAP
		for (PredicateMap pm : pom.getPredicateMaps()) {

			RDFTerm pmConstant = pm.getConstant();
			if (pmConstant != null) {
				IRIConstant bodyPredicate = termFactory.getConstantIRI(
						rdfFactory.createIRI(pmConstant.toString()));
				predicateAtoms.add(bodyPredicate);
			}

			Template t = pm.getTemplate();
			if (t != null) {
				// create uri("...",var)
				NonVariableTerm predicateAtom = getURIFunction(t.toString());
				predicateAtoms.add(predicateAtom);
			}

			// process column declaration
			String c = pm.getColumn();
			if (c != null) {
				NonVariableTerm predicateAtom = getURIFunction(c);
				predicateAtoms.add(predicateAtom);
			}
		}
		return predicateAtoms;

	}

	public ImmutableTerm getObjectAtom(PredicateObjectMap pom) throws InvalidR2RMLMappingException {
		return getObjectAtom(pom, "");
	}

	public boolean isConcat(String st) {
		int i, j;
		if ((i = st.indexOf("{")) > -1) {
			if ((j = st.lastIndexOf("{")) > i) {
				return true;
			} else if ((i > 0) || ((j > 0) && (j < (st.length() - 1)))) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Get the object atom, it can be a constant, a column or a template
	 * 
	 * @param pom
	 * @param joinCond
	 * @return
	 * @throws Exception
	 */
	public ImmutableTerm getObjectAtom(PredicateObjectMap pom, String joinCond) throws InvalidR2RMLMappingException {
		ImmutableTerm lexicalTerm = null;
		if (pom.getObjectMaps().isEmpty()) {
			return null;
		}
		ObjectMap om = pom.getObjectMap(0);

		String lan = om.getLanguageTag();
		IRI datatype = om.getDatatype();

		// we check if the object map is a constant (can be a iri or a literal)
        // TODO(xiao): toString() is suspicious
        RDFTerm constantObj = om.getConstant();
		if (constantObj != null) {
			// boolean isURI = false;
			// try {
			// java.net.URI.create(obj);
			// isURI = true;
			// } catch (IllegalArgumentException e){
			//
			// }

			// if the literal has a language property or a datatype property we
			// create the function object later

			if (lan != null || datatype != null) {
				lexicalTerm = termFactory.getDBStringConstant(((Literal) constantObj).getLexicalForm());

			} else {

				if (constantObj instanceof Literal){

					String lexicalString = ((Literal) constantObj).getLexicalForm();
					lexicalTerm = termFactory.getDBStringConstant(lexicalString);
					Literal constantLit1 = (Literal) constantObj;

					String lanConstant = om.getLanguageTag();
					IRI datatypeConstant = constantLit1.getDatatype();

					// we check if it is a literal with language tag

					if (lanConstant != null) {
						return termFactory.getRDFLiteralFunctionalTerm(lexicalTerm, lanConstant);
					}

					// we check if it is a typed literal
					else if (datatypeConstant != null) {
						if ((!settings.areAbstractDatatypesToleratedInMapping())
							&& typeFactory.getDatatype(datatypeConstant).isAbstract())
							throw new InvalidR2RMLMappingException("Abstract datatype "
									+datatypeConstant  + " detected in the mapping assertion.\nSet the property "
									+ OntopMappingSettings.TOLERATE_ABSTRACT_DATATYPE + " to true to tolerate them.");
						return termFactory.getRDFLiteralFunctionalTerm(lexicalTerm, datatypeConstant);
					}
					else {
						// Use RDFS.LITERAL when the datatype is not specified (-> to be inferred)
						return termFactory.getRDFLiteralConstant(lexicalString, RDFS.LITERAL);
					}
                } else if (constantObj instanceof IRI){
                    return termFactory.getConstantIRI((IRI) constantObj);
                }
			}
		}

		// we check if the object map is a column
		// if it has a datatype or language property or its a iri we check it later
		String col = om.getColumn();
		if (col != null) {
			col = trim(col);

			if (!joinCond.isEmpty()) {
				col = joinCond + col;
			}

			lexicalTerm = termFactory.getPartiallyDefinedToStringCast(termFactory.getVariable(col));

		}

		// we check if the object map is a template (can be a iri, a literal or
		// a blank node)
		Template t = om.getTemplate();
		IRI typ = om.getTermType();
		boolean concat = false;
		if (t != null) {
			//we check if the template is a literal
			//then we check if the template includes concat 
			concat = isConcat(t.toString());
			if (typ.equals(R2RMLVocabulary.literal) && (concat)){
				lexicalTerm = getTypedFunction(t.toString(), 4, joinCond);
			}else {

				// a template can be a rr:IRI, a
				// rr:Literal or rr:BlankNode

				// if the literal has a language property or a datatype property
				// we
				// create the function object later
				if (lan != null || datatype != null) {
					String value = t.getColumnName(0);
					if (!joinCond.isEmpty()) {
						value = joinCond + value;

					}
					lexicalTerm = termFactory.getPartiallyDefinedToStringCast(termFactory.getVariable(value));
				} else {
					IRI type = om.getTermType();

					// we check if the template is a IRI a simple literal or a
					// blank
					// node and create the function object
					lexicalTerm = getTermTypeAtom(t.toString(), type, joinCond);
				}
			}
		}
		else{
			//assign iri template
			TermMap.TermMapType termMapType = om.getTermMapType();
			if(termMapType.equals(TermMap.TermMapType.CONSTANT_VALUED)){

			} else if(termMapType.equals(TermMap.TermMapType.COLUMN_VALUED)){
				if(typ.equals(R2RMLVocabulary.iri)) {
					// Cast to Variable added. TODO: check
					return termFactory.getIRIFunctionalTerm((Variable) lexicalTerm, true);
				}
			}

		}

		// we check if it is a literal with language tag

		if (lan != null) {
			return termFactory.getRDFLiteralFunctionalTerm(lexicalTerm, lan);
		}

		// we check if it is a typed literal
		if (datatype != null) {
			return termFactory.getRDFLiteralFunctionalTerm(lexicalTerm, datatype);
		}

		if (typ.equals(R2RMLVocabulary.literal)) {
			return termFactory.getRDFLiteralFunctionalTerm(lexicalTerm, RDFS.LITERAL);
		}

		throw new MinorOntopInternalBugException("TODO: fix this broken logic for " + lexicalTerm);
	}

	@Deprecated
	public List<BlankNodeOrIRI> getJoinNodes(TriplesMap tm) {
		List<BlankNodeOrIRI> joinPredObjNodes = new ArrayList<BlankNodeOrIRI>();
		// get predicate-object nodes
		Set<BlankNodeOrIRI> predicateObjectNodes = getPredicateObjects(tm);
		return joinPredObjNodes;
	}

	/**
	 * get a typed atom of a specific type
	 * 
	 * @param type
	 *            - iri, blanknode or literal
	 * @param string
	 *            - the atom as string
	 * @return the contructed Function atom
	 */
	private NonVariableTerm getTermTypeAtom(String string, Object type, String joinCond) {

		if (type.equals(R2RMLVocabulary.iri)) {

			return getURIFunction(string, joinCond);

		} else if (type.equals(R2RMLVocabulary.blankNode)) {

			return getTypedFunction(string, 2, joinCond);

		} else if (type.equals(R2RMLVocabulary.literal)) {

			return getTypedFunction(trim(string), 3, joinCond);
		}
		return null;
	}

	private NonVariableTerm getURIFunction(String string, String joinCond) {
		return getTypedFunction(string, 1, joinCond);
	}

	private NonVariableTerm getURIFunction(String string) {
		return getTypedFunction(string, 1);
	}

	public NonVariableTerm getTypedFunction(String parsedString, int type) {
		return getTypedFunction(parsedString, type, "");
	}
	
	
	//this function distinguishes curly bracket with back slash "\{" from curly bracket "{" 
	private int getIndexOfCurlyB(String str){
		int i;
		int j;
		i = str.indexOf("{");
		j = str.indexOf("\\{");
		
		while((i-1 == j) && (j != -1)){		
			i = str.indexOf("{",i+1);
			j = str.indexOf("\\{",j+1);		
		}	
		return i;
	}

	/**
	 * get a typed atom
	 * 
	 * @param parsedString
	 *            - the content of atom
	 * @param type
	 *            - 0=constant uri, 1=uri or iri, 2=bnode, 3=literal 4=concat
	 * @param joinCond
	 *            - CHILD_ or PARENT_ prefix for variables
	 * @return the constructed Function atom
	 */
	public NonVariableTerm getTypedFunction(String parsedString, int type,
			String joinCond) {

		List<ImmutableTerm> terms = new ArrayList<>();
		String string = (parsedString);
		if (!string.contains("{")) {
			if (type < 3) {
    				if (!R2RMLVocabulary.isResourceString(string)) {
						string = R2RMLVocabulary.prefixUri("{" + string + "}");
					if (type == 2) {
						string = "\"" + string + "\"";
					}
				} else {
					type = 0;
				}
			}
		}
		if (type == 1) {
			string = R2RMLVocabulary.prefixUri(string);
		}

		String str = string; //str for concat of constant literal
		
		string = string.replace("\\{", "[");
		string = string.replace("\\}", "]");
		
		String cons;
		int i;
		while (string.contains("{")) {
			int end = string.indexOf("}");
			int begin = string.lastIndexOf("{", end);
			
			// (Concat) if there is constant literal in template, adds it to terms list 
			if (type == 4){
				if ((i = getIndexOfCurlyB(str)) > 0){
					cons = str.substring(0, i);
					str = str.substring(str.indexOf("}", i)+1, str.length());
					terms.add(termFactory.getDBStringConstant(cons));
				}else{
					str = str.substring(str.indexOf("}")+1);
				}
			}

			String var = trim(string.substring(begin + 1, end));

			// trim for making variable
			terms.add(termFactory.getPartiallyDefinedToStringCast(termFactory.getVariable(joinCond + (var))));

			string = string.replaceFirst("\\{\"" + var + "\"\\}", "[]");
			string = string.replaceFirst("\\{" + var + "\\}", "[]");
			
		}
		if(type == 4){
			if (!str.equals("")){
				cons = str;
				terms.add(termFactory.getDBStringConstant(cons));
			}
		}
	
		string = string.replace("[", "{");
		string = string.replace("]", "}");

		switch (type) {
		// constant uri
		case 0:
			IRI iri = rdfFactory.createIRI(string);
			return termFactory.getConstantIRI(iri);
			// URI or IRI
		case 1:
			return termFactory.getIRIFunctionalTerm(string, ImmutableList.copyOf(terms));
			// BNODE
		case 2:
			return termFactory.getBnodeFunctionalTerm(string, ImmutableList.copyOf(terms));
			// simple LITERAL
		case 3:
			ImmutableTerm lexicalValue = terms.remove(0);
			// pred = typeFactory.getRequiredTypePredicate(); //
			// the URI template is always on the first position in the term list
			// terms.add(0, uriTemplate);
			return termFactory.getRDFLiteralFunctionalTerm(lexicalValue, XSD.STRING);
		case 4://concat
			return termFactory.getDBConcatFunctionalTerm(ImmutableList.copyOf(terms));
		}
		return null;
	}

	/**
	 * method that trims a string of all its double apostrophes from beginning
	 * and end
	 * 
	 * @param string
	 *            - to be trimmed
	 * @return the string without any quotes
	 */
	private String trim(String string) {

		while (string.startsWith("\"") && string.endsWith("\"")) {

			string = string.substring(1, string.length() - 1);
		}
		return string;
	}

	/**
	 * method to trim a string of its leading or trailing quotes but one
	 * 
	 * @param string
	 *            - to be trimmed
	 * @return the string left with one leading and trailing quote
	 */
	private String trimTo1(String string) {

		while (string.startsWith("\"\"") && string.endsWith("\"\"")) {

			string = string.substring(1, string.length() - 1);
		}
		return string;
	}


}
