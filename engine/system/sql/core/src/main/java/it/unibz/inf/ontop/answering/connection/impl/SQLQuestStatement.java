package it.unibz.inf.ontop.answering.connection.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import it.unibz.inf.ontop.answering.connection.JDBCStatementFinalizer;
import it.unibz.inf.ontop.answering.logging.QueryLogger;
import it.unibz.inf.ontop.answering.reformulation.input.*;
import it.unibz.inf.ontop.answering.resultset.GraphResultSet;
import it.unibz.inf.ontop.answering.resultset.impl.*;
import it.unibz.inf.ontop.answering.resultset.BooleanResultSet;
import it.unibz.inf.ontop.answering.resultset.TupleResultSet;
import it.unibz.inf.ontop.exception.*;
import it.unibz.inf.ontop.injection.OntopSystemSQLSettings;
import it.unibz.inf.ontop.answering.resultset.impl.PredefinedBooleanResultSet;

import it.unibz.inf.ontop.answering.reformulation.QueryReformulator;
import it.unibz.inf.ontop.iq.IQ;
import it.unibz.inf.ontop.iq.IQTree;
import it.unibz.inf.ontop.iq.UnaryIQTree;
import it.unibz.inf.ontop.iq.exception.EmptyQueryException;
import it.unibz.inf.ontop.iq.node.ConstructionNode;
import it.unibz.inf.ontop.iq.node.NativeNode;
import it.unibz.inf.ontop.model.term.TermFactory;
import it.unibz.inf.ontop.model.term.Variable;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.substitution.SubstitutionFactory;
import org.apache.commons.rdf.api.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.ResultSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL-specific implementation of OBDAStatement.
 * Derived from QuestStatement.
 */
public class SQLQuestStatement extends QuestStatement {

    private final Statement sqlStatement;
    private final JDBCStatementFinalizer statementFinalizer;
    private final TermFactory termFactory;
    private final RDF rdfFactory;
    private final SubstitutionFactory substitutionFactory;
    private final OntopSystemSQLSettings settings;
    private List<String> drop_stream=new ArrayList<>();

    private static final Logger log = LoggerFactory.getLogger(QuestStatement.class);

    public SQLQuestStatement(QueryReformulator queryProcessor, Statement sqlStatement,
                             JDBCStatementFinalizer statementFinalizer, TermFactory termFactory,
                             RDF rdfFactory, SubstitutionFactory substitutionFactory,
                             OntopSystemSQLSettings settings) {
        super(queryProcessor);
        this.sqlStatement = sqlStatement;
        this.statementFinalizer = statementFinalizer;
        this.termFactory = termFactory;
        this.rdfFactory = rdfFactory;
        this.substitutionFactory = substitutionFactory;
        this.settings = settings;
    }

    @Override
    public int getMaxRows() throws OntopConnectionException {
        try {
            return sqlStatement.getMaxRows();
        } catch (SQLException e) {
            throw new OntopConnectionException(e);
        }

    }

    @Override
    public void getMoreResults() throws OntopConnectionException {
        try {
            sqlStatement.getMoreResults();
        } catch (SQLException e) {
            throw new OntopConnectionException(e);
        }

    }

    @Override
    public void setMaxRows(int max) throws OntopConnectionException {
        try {
            sqlStatement.setMaxRows(max);
        } catch (SQLException e) {
            throw new OntopConnectionException(e);
        }

    }

    @Override
    public void setQueryTimeout(int seconds) throws OntopConnectionException {
        try {
            sqlStatement.setQueryTimeout(seconds);
        } catch (SQLException e) {
            throw new OntopConnectionException(e);
        }
    }

    @Override
    public int getQueryTimeout() throws OntopConnectionException {
        try {
            return sqlStatement.getQueryTimeout();
        } catch (SQLException e) {
            throw new OntopConnectionException(e);
        }
    }

    @Override
    public boolean isClosed() throws OntopConnectionException {
        try {
            return sqlStatement.isClosed();
        } catch (SQLException e) {
            throw new OntopConnectionException(e);
        }
    }

    /**
     * Returns the number of tuples returned by the query
     */
    @Override
    public int getTupleCount(InputQuery inputQuery) throws OntopReformulationException, OntopQueryEvaluationException {
        IQ targetQuery = getExecutableQuery(inputQuery);
        try {
            String sql = extractSQLQuery(targetQuery);
            String newsql = "SELECT count(*) FROM (" + sql + ") t1";
            if (!isCanceled()) {
                try {

                    java.sql.ResultSet set = sqlStatement.executeQuery(newsql);
                    if (set.next()) {
                        return set.getInt(1);
                    } else {
                        //throw new OBDAException("Tuple count failed due to empty result set.");
                        return 0;
                    }
                } catch (SQLException e) {
                    throw new OntopQueryEvaluationException(e);
                }
            } else {
                throw new OntopQueryEvaluationException("Action canceled.");
            }
        } catch (EmptyQueryException e) {
            return 0;
        }
    }

    @Override
    public void close() throws OntopConnectionException {
        try {
            if (sqlStatement != null)
                statementFinalizer.closeStatement(sqlStatement);
        } catch (SQLException e) {
            throw new OntopConnectionException(e);
        }
    }

    protected void cancelExecution() throws OntopQueryEvaluationException {
        try {
            sqlStatement.cancel();
        } catch (SQLException e) {
            throw new OntopQueryEvaluationException(e);
        }
    }

    @Override
    public BooleanResultSet executeBooleanQuery(IQ executableQuery, QueryLogger queryLogger)
            throws OntopQueryEvaluationException {
        try {
            String sqlQuery = extractSQLQuery(executableQuery);
            try {
                java.sql.ResultSet set = sqlStatement.executeQuery(sqlQuery);
                queryLogger.declareResultSetUnblockedAndSerialize();
                return new SQLBooleanResultSet(set, queryLogger, this::close);
            } catch (SQLException e) {
                throw new OntopQueryEvaluationException(e.getMessage());
            }
        } catch (EmptyQueryException e) {
            queryLogger.declareResultSetUnblockedAndSerialize();
            return new PredefinedBooleanResultSet(false);
        }
    }

    @Override
    protected TupleResultSet executeSelectQuery(IQ executableQuery, QueryLogger queryLogger,
                                                boolean shouldAlsoCloseStatement)
            throws OntopQueryEvaluationException {
        try {
            String sqlQuery = extractSQLQuery(executableQuery);
            ConstructionNode constructionNode = extractRootConstructionNode(executableQuery);
            NativeNode nativeNode = extractNativeNode(executableQuery);
            ImmutableSortedSet<Variable> signature = nativeNode.getVariables();
            ImmutableMap<Variable, DBTermType> typeMap = nativeNode.getTypeMap();

            OntopConnectionCloseable statementClosingCB = shouldAlsoCloseStatement ? this::close : null;

            try {

                //try something for ksql
                sqlQuery = transformSQLIntoKSQL(sqlQuery);

                java.sql.ResultSet set = sqlStatement.executeQuery(sqlQuery);
                queryLogger.declareResultSetUnblockedAndSerialize();

                // only needed for kafka..
                if (set.next() == false) {
                    //ResultSet is empty
                } else {
                    do {
                        set.getString(1);
                    } while (set.next());
                }

                drop_tmpStreams();

                return settings.isDistinctPostProcessingEnabled()
                        ? new DistinctJDBCTupleResultSet(set, signature, typeMap, constructionNode,
                            executableQuery.getProjectionAtom(), queryLogger, statementClosingCB, termFactory, substitutionFactory)
                        : new JDBCTupleResultSet(set, signature, typeMap, constructionNode, executableQuery.getProjectionAtom(),
                            queryLogger, statementClosingCB, termFactory, substitutionFactory);
            } catch (SQLException e) {
                throw new OntopQueryEvaluationException(e);
            }
        } catch (EmptyQueryException e) {
            queryLogger.declareResultSetUnblockedAndSerialize();
            return new EmptyTupleResultSet(executableQuery.getProjectionAtom().getArguments(), queryLogger);
        }
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// For now Kafka part implemented here; todo: find better place for it, maybe own file.
////
    private String transformSQLIntoKSQL(String sql) throws OntopQueryEvaluationException {

        // Check if query is nested
        boolean isNestedQuery = (sql.split("SELECT", -1).length-1) > 1? true : false;

        // Remove nulls in query
        sql = removeNull(sql);

        // Rewrite nested query
        if(isNestedQuery){
            sql = rewriteSql(sql);
        }else{
            sql = sql + "EMIT CHANGES LIMIT 1;";
        }

        // Result
        return sql;
    }

    private String removeNull(String sql){

        // Remove all nulls
        BufferedReader buffReader = new BufferedReader(new StringReader(sql));
        String line = null;
        Matcher matcher;

        try{
            while( (line=buffReader.readLine()) != null )
            {
                // remove null, and, if null in where and no end, remove complete where
                boolean isNullQuery = (line.split("NULL", -1).length-1) >= 1? true : false;
                boolean isNullAndQuery = (line.split("NULL AND", -1).length-1) >= 1? true : false;

                boolean isCaseNull = (line.split("ELSE NULL", -1).length-1) >= 1? true : false;

                if(isNullQuery){
                    if(isCaseNull){
                        sql.replaceAll("ELSE NULL", "");
                    }else if(isNullAndQuery){
                        //remove null part
                        String null_part = (
                                (matcher = Pattern.compile("WHERE (.* NULL AND)?").matcher(line))
                                        .find() ? matcher.group(1) : null);
                        sql = sql.replaceAll(null_part, "");
                    }else{
                        //remove full where clause
                        String null_where = (
                                (matcher = Pattern.compile("WHERE .* NULL").matcher(line))
                                        .find() ? matcher.group() : null);
                        sql = sql.replaceAll(null_where, "");
                    }
                }
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
        log.debug("//KSQL-after NULL removed");
        log.debug(sql);
        log.debug("/////////////////////////");
        return sql;
    }

    private String rewriteSql(String sql){
        // change pattern matching. matching result contains to be stream name
        List<String> sub_streams=new ArrayList<String>();
        List<String> sub_streamNames=new ArrayList<String>();
        int index = 0;
        int stream_amount = 0;

        String mydata = sql.replaceAll("(\\t|\\r?\\n)+", " ");
        String myoriginaldata = mydata;
        Pattern pattern = Pattern.compile("\\(SELECT .*\\)");
        Matcher matcher = pattern.matcher(mydata);
        while (matcher.find())
        {
            mydata = matcher.group(0);
        }
        pattern = Pattern.compile("\\) (`.*?`)");
        matcher = pattern.matcher(mydata);
        while (matcher.find())
        {
            sub_streamNames.add(matcher.group(1));

            sub_streams.add("CREATE STREAM "+ matcher.group(1) +" AS ");
            index = index + 1;
        }
        index = 0;
        pattern = Pattern.compile("(SELECT '.*?' AS `..` )");
        matcher = pattern.matcher(mydata);
        while (matcher.find())
        {
            sub_streams.set(index, sub_streams.get(index) + matcher.group(1));
            index = index + 1;
        }
        index = 0;
        Pattern pattern2 = Pattern.compile("SELECT 1 AS uselessVariable (.*?\\) )");
        matcher = pattern2.matcher(mydata);
        while (matcher.find())
        {
            String foo = matcher.group(1).replaceAll("\\(.*`ROWKEY` AND", "(");
            sub_streams.set(index, sub_streams.get(index) + foo);
            index = index + 1;
        }
        while (index > 0)
        {
            index = index - 1;
            sub_streams.set(index, sub_streams.get(index).replaceAll("(\\()+", " ").replaceAll("(\\))+", " ") + "EMIT CHANGES LIMIT 1;");
            stream_amount = stream_amount + 1;
        }

        String main_stream = (
                (matcher = Pattern.compile(".*(`.*?`)").matcher(myoriginaldata))
                        .find() ? matcher.group(1) : null);

        //redo
        String select_name = (
                (matcher = Pattern.compile("AS (`.*?`)").matcher(mydata))
                        .find() ? matcher.group(1) : null);
        //redo
        String select_name_first = (
                (matcher = Pattern.compile("(`..?`) WHERE").matcher(mydata))
                        .find() ? matcher.group(1) : null);

        // Create temporary streams
        createTmpStream(sub_streams, main_stream, select_name, select_name_first, sub_streamNames);

        // Select
        String ksql = String.format("SELECT %s FROM %s EMIT CHANGES LIMIT %d;", select_name_first, main_stream, stream_amount);

        return ksql;
    }

    private void createTmpStream(List<String> sub_streams, String main_stream, String select_name, String select_name_first, List<String> sub_streamNames){

        // create tmp stream
        int tmp_counter = 0;
        while (tmp_counter < sub_streamNames.size())
        {
            try{
                sqlStatement.executeQuery(sub_streams.get(tmp_counter));
                drop_stream.add(String.format("DROP STREAM %s", sub_streamNames.get(tmp_counter)));
                if(tmp_counter == 0){
                    // Create Query Stream
                    sqlStatement.executeQuery(String.format("CREATE STREAM %s AS SELECT %s as %s FROM %s EMIT CHANGES LIMIT 1;", main_stream, select_name, select_name_first, sub_streamNames.get(tmp_counter)));
                    drop_stream.add(String.format("DROP STREAM %s", main_stream));
                }else{
                    // Insert
                    sqlStatement.executeQuery(String.format("INSERT INTO %s SELECT %s as %s FROM %s EMIT CHANGES LIMIT 1;", main_stream, select_name, select_name_first, sub_streamNames.get(tmp_counter)));
                }
                tmp_counter = tmp_counter + 1;
            }catch (SQLException ex){
                ex.printStackTrace();
            }
        }
    }

    private void drop_tmpStreams(){

        // After the sql is executed we have to clear the created streams
        // Terminate the running queries first and then drop the streams

        Pattern pattern = Pattern.compile(": \\[(.*?)\\]");

        // Get terminate msg
        drop_stream.stream().forEach((stream_name) -> {
            try {
                sqlStatement.executeQuery(String.format("%s", stream_name));
            } catch (SQLException ex) {
                Matcher matcher = pattern.matcher(ex.getMessage().replaceAll("\\[\\]", ""));
                while (matcher.find())
                {
                    List<String> terminate_list = Arrays.asList(matcher.group(1).split(","));
                    terminate_list.stream().forEach((query_name) -> {
                        try {
                            sqlStatement.executeQuery(String.format("TERMINATE %s", query_name));
                        } catch (SQLException ex2) {
                            ex2.printStackTrace();
                        }
                    });
                }
            }
        });
        // Drop streams
        drop_stream.stream().forEach((stream_name) -> {
            try {
                sqlStatement.executeQuery(String.format("%s", stream_name));
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        });
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * TODO: make it SQL-independent
     */
    @Override
    protected GraphResultSet executeConstructQuery(ConstructTemplate constructTemplate, IQ executableQuery, QueryLogger queryLogger,
                                                   boolean shouldAlsoCloseStatement)
            throws OntopQueryEvaluationException, OntopResultConversionException, OntopConnectionException {
        TupleResultSet tuples;
        try {
            String sqlQuery = extractSQLQuery(executableQuery);
            ConstructionNode constructionNode = extractRootConstructionNode(executableQuery);
            NativeNode nativeNode = extractNativeNode(executableQuery);
            ImmutableSortedSet<Variable> SQLSignature = nativeNode.getVariables();
            ImmutableMap<Variable, DBTermType> SQLTypeMap = nativeNode.getTypeMap();

            OntopConnectionCloseable statementClosingCB = shouldAlsoCloseStatement ? this::close : null;

            try {
                ResultSet rs = sqlStatement.executeQuery(sqlQuery);
                queryLogger.declareResultSetUnblockedAndSerialize();
                tuples = new JDBCTupleResultSet(rs, SQLSignature, SQLTypeMap, constructionNode,
                        executableQuery.getProjectionAtom(), queryLogger, statementClosingCB, termFactory, substitutionFactory);
            } catch (SQLException e) {
                throw new OntopQueryEvaluationException(e.getMessage());
            }
        } catch (EmptyQueryException e) {
            queryLogger.declareResultSetUnblockedAndSerialize();
            tuples = new EmptyTupleResultSet(executableQuery.getProjectionAtom().getArguments(), queryLogger);
        }
        return new DefaultSimpleGraphResultSet(tuples, constructTemplate, termFactory, rdfFactory,
                settings.areInvalidTriplesExcludedFromResultSet());
    }

    private NativeNode extractNativeNode(IQ executableQuery) throws EmptyQueryException {
        IQTree tree = executableQuery.getTree();
        if (tree.isDeclaredAsEmpty()) {
            throw new EmptyQueryException();
        }
        return Optional.of(tree)
                .filter(t -> t instanceof UnaryIQTree)
                .map(t -> ((UnaryIQTree)t).getChild().getRootNode())
                .filter(n -> n instanceof NativeNode)
                .map(n -> (NativeNode) n)
                .orElseThrow(() -> new MinorOntopInternalBugException("The query does not have the expected structure " +
                        "for an executable query\n" + executableQuery));
    }

    private String extractSQLQuery(IQ executableQuery) throws EmptyQueryException, OntopInternalBugException {
        IQTree tree = executableQuery.getTree();
        if  (tree.isDeclaredAsEmpty())
            throw new EmptyQueryException();

        String queryString = Optional.of(tree)
                .filter(t -> t instanceof UnaryIQTree)
                .map(t -> ((UnaryIQTree)t).getChild().getRootNode())
                .filter(n -> n instanceof NativeNode)
                .map(n -> (NativeNode) n)
                .map(NativeNode::getNativeQueryString)
                .orElseThrow(() -> new MinorOntopInternalBugException("The query does not have the expected structure " +
                        "of an executable query\n" + executableQuery));

        if (queryString.equals(""))
            throw new EmptyQueryException();

        return queryString;
    }

    private ConstructionNode extractRootConstructionNode(IQ executableQuery) throws EmptyQueryException, OntopInternalBugException {
        IQTree tree = executableQuery.getTree();
        if  (tree.isDeclaredAsEmpty())
            throw new EmptyQueryException();

        return Optional.of(tree.getRootNode())
                .filter(n -> n instanceof ConstructionNode)
                .map(n -> (ConstructionNode)n)
                .orElseThrow(() -> new MinorOntopInternalBugException(
                        "The \"executable\" query is not starting with a construction node\n" + executableQuery));
    }
}
