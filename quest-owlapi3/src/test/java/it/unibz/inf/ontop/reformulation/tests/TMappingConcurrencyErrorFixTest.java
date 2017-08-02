package it.unibz.inf.ontop.reformulation.tests;

/*
 * #%L
 * ontop-test
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

import it.unibz.inf.ontop.injection.OntopSQLOWLAPIConfiguration;
import it.unibz.inf.ontop.owlrefplatform.owlapi.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.semanticweb.owlapi.model.OWLIndividual;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static it.unibz.inf.ontop.injection.OntopMappingSettings.OBTAIN_FULL_METADATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/***
 * Tests that TMapping does not return error in case of symmetric properties.
 * Use to check that no concurrency error appears. 
 */
public class TMappingConcurrencyErrorFixTest{
	private OntopOWLConnection conn;
	private Connection connection;
	

	Logger log = LoggerFactory.getLogger(this.getClass());

	final String owlFileName = "src/test/resources/test/tmapping/exampleTMapping.owl";
	final String obdaFileName = "src/test/resources/test/tmapping/exampleTMapping.obda";
	private QuestOWL reasoner;

	@Before
	public void setUp() throws Exception {
		
		
		// String driver = "org.h2.Driver";
		String url = "jdbc:h2:mem:questjunitdb;";
		String username = "sa";
		String password = "";

		connection = DriverManager.getConnection(url, username, password);
		Statement st = connection.createStatement();

		FileReader reader = new FileReader("src/test/resources/test/tmapping/create-tables.sql");
		BufferedReader in = new BufferedReader(reader);
		StringBuilder bf = new StringBuilder();
		String line = in.readLine();
		while (line != null) {
			bf.append(line);
			line = in.readLine();
		}

		st.executeUpdate(bf.toString());
		connection.commit();
	
		Properties p = new Properties();
		p.put(OBTAIN_FULL_METADATA, false);
		// Creating a new instance of the reasoner
        QuestOWLFactory factory = new QuestOWLFactory();
        OntopSQLOWLAPIConfiguration config = OntopSQLOWLAPIConfiguration.defaultBuilder()
				.nativeOntopMappingFile(obdaFileName)
				.ontologyFile(owlFileName)
				.properties(p)
				.jdbcUrl(url)
				.jdbcUser(username)
				.jdbcPassword(password)
				.enableTestMode()
				.build();
        reasoner = factory.createReasoner(config);

		// Now we are ready for querying
		conn = reasoner.getConnection();
	}

	@After
	public void tearDown() throws Exception{
	
			dropTables();
			reasoner.dispose();
			connection.close();
			
		
		
	}
	
	private void dropTables() throws SQLException, IOException {

		Statement st = connection.createStatement();

		FileReader reader = new FileReader("src/test/resources/test/tmapping/drop-tables.sql");
		BufferedReader in = new BufferedReader(reader);
		StringBuilder bf = new StringBuilder();
		String line = in.readLine();
		while (line != null) {
			bf.append(line);
			line = in.readLine();
		}

		st.executeUpdate(bf.toString());
		st.close();
		connection.commit();
	}
	
	private String runTests(String query) throws Exception {
		OntopOWLStatement st = conn.createStatement();
		String retval=null;
		try {
			QuestOWLResultSet rs = st.executeSelectQuery(query);
			assertTrue(rs.nextRow());
			OWLIndividual ind1 =	rs.getOWLIndividual("y")	 ;
			retval = ind1.toString();
			assertEquals("<http://www.semanticweb.org/sarah/ontologies/2014/4/untitled-ontology-73#111>", retval);
			assertTrue(rs.nextRow());
			OWLIndividual ind2 =	rs.getOWLIndividual("y")	 ;
			retval = ind2.toString();
			assertEquals("<http://www.semanticweb.org/sarah/ontologies/2014/4/untitled-ontology-73#112>", retval);
		} catch (Exception e) {
			throw e;
		} finally {
			try {

			} catch (Exception e) {
				st.close();
				throw e;
			}
			
			conn.close();
			reasoner.dispose();
		}
		return retval;
	}

	/**
	 * Test no error is generate before SPARQL query 
	 * @throws Exception
	 */
	@Test
	public void test() throws Exception {
		String query = "PREFIX  : <http://www.semanticweb.org/sarah/ontologies/2014/4/untitled-ontology-73#> SELECT ?y WHERE { ?y a :Man }";
		String val = runTests(query);
		
	}
	


			
}