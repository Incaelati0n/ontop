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
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
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

    private List<String> create_stream=new ArrayList<>();
    private List<String> insert_stream=new ArrayList<>();
    private List<String> drop_stream=new ArrayList<>();
    private boolean isMainSelect = true;
    private String mainStream_name;
    private String mainStream_body;
    private String main_select;

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
                java.sql.ResultSet set;
                if((settings.getJdbcDriver().equals("com.github.mmolimar.ksql.jdbc.KsqlDriver"))){
                    sqlQuery = transformSQLIntoKSQL(sqlQuery);
                    set = sqlStatement.executeQuery(sqlQuery);
                    if (set.next() == false) {
                        //ResultSet is empty
                    } else {
                        do {
                            set.getString(1);
                        } while (set.next());
                    }
                    //executeAdditionalQuery(-1);
                }else{
                    set = sqlStatement.executeQuery(sqlQuery);
                }
                queryLogger.declareResultSetUnblockedAndSerialize();

                return settings.isDistinctPostProcessingEnabled()
                        ? new DistinctJDBCTupleResultSet(set, signature, typeMap, constructionNode,
                            executableQuery.getProjectionAtom(), queryLogger, statementClosingCB, termFactory, substitutionFactory)
                        : new JDBCTupleResultSet(set, signature, typeMap, constructionNode, executableQuery.getProjectionAtom(),
                            queryLogger, statementClosingCB, termFactory, substitutionFactory);
            } catch (SQLException | JSQLParserException e) {
                throw new OntopQueryEvaluationException(e);
            }
        } catch (EmptyQueryException e) {
            queryLogger.declareResultSetUnblockedAndSerialize();
            return new EmptyTupleResultSet(executableQuery.getProjectionAtom().getArguments(), queryLogger);
        }
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /** For now Kafka part is implemented here in this file;
     * todo: find better place for ksql impl. (own file?)
     */

    private void rewriteSqltoKsql(String plainSelect){

        plainSelect = plainSelect.replaceAll("\\(.*`ROWKEY` AND", "(");

        Pattern pattern = Pattern.compile("`[^`]*`\\.");
        Matcher matcher = pattern.matcher(plainSelect);

        while(matcher.find()) {
            plainSelect = plainSelect.replace(matcher.group(),"");
        }

        plainSelect = plainSelect.replace("ELSE NULL", "ELSE ''");

        boolean isSingleSelect = (plainSelect.split("SELECT", -1).length-1) == 1? true : false;
        if(!isSingleSelect){
            plainSelect = plainSelect.replace((
                    (matcher = Pattern.compile("(\\(SELECT.*FROM `.*?`)").matcher(plainSelect))
                            .find() ? matcher.group(1) : ""),"");
            if(!isMainSelect){
                plainSelect = plainSelect.replace((
                        (matcher = Pattern.compile("(WHERE.*)\\)").matcher(plainSelect))
                                .find() ? matcher.group(1) : ""),"");
            }else{
                plainSelect = plainSelect.replace((
                        (matcher = Pattern.compile(".*(`.*` WHERE.*?\\))").matcher(plainSelect))
                                .find() ? matcher.group(1) : ""),"");
            }
        }
        String select_body = (
                (matcher = Pattern.compile("(.*)`v.*?`(.*)").matcher(plainSelect))
                        .find() ? matcher.group(1) + matcher.group(2) : null);

        String select_name = (
                (matcher = Pattern.compile(".*(`v.*?`).*").matcher(plainSelect))
                        .find() ? matcher.group(1) : null);

        if((select_body == null) || (select_name == null)){
            //
        }else{
            if(select_body.endsWith(") "))
            {
                select_body = select_body.substring(0,select_body.length() - 2);
            }
            if(isMainSelect){
                select_body = select_body.replace((
                        (matcher = Pattern.compile(".*FROM.*?(`.*?`\\))").matcher(select_body))
                                .find() ? matcher.group(1) : ""),"");
                mainStream_name = select_name;
                mainStream_body = select_body.replaceAll("FROM.*", "");
                String obj = ((matcher = Pattern.compile(".*(`v.*?`).*").matcher(mainStream_body)).find() ? matcher.group(1) : null);
                main_select = mainStream_body + " FROM " + mainStream_name + " WHERE " + obj + " IS NOT NULL EMIT CHANGES LIMIT 1;";
                isMainSelect = false;
            }else{
                if(!isSingleSelect) {
                    String sub_insertQuery = "INSERT INTO " + mainStream_name + " " + mainStream_body + " FROM " + select_name;
                    insert_stream.add(sub_insertQuery);
                    int limit_size = insert_stream.size();
                    String obj = ((matcher = Pattern.compile(".*(`v.*?`).*").matcher(mainStream_body)).find() ? matcher.group(1) : null);
                    main_select = mainStream_body + " FROM " + mainStream_name + " WHERE " + obj + " IS NOT NULL EMIT CHANGES LIMIT " + limit_size;
                }
            }
            String sub_streamQuery = "CREATE STREAM " + select_name + " AS " + select_body;
            String sub_dropQuery = "DROP STREAM " + select_name;
            create_stream.add(sub_streamQuery);
            drop_stream.add(sub_dropQuery);
        }
    }

    private void iterateNestedQuery(String sql) throws JSQLParserException {
        Select select = (Select) CCJSqlParserUtil.parse(sql);

        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder() {
            @Override
            public void visit(PlainSelect plainSelect) {
                rewriteSqltoKsql(plainSelect.toString());
                super.visit(plainSelect);
            }
        };
        tablesNamesFinder.getTableList(select);
    }

    private String transformSQLIntoKSQL(String sql) throws JSQLParserException {

        // Check if query is nested
        boolean isNestedQuery = (sql.split("SELECT", -1).length-1) > 1? true : false;

        // Rewrite nested query
        if(isNestedQuery){
            iterateNestedQuery(sql);
            //drop streams
            executeAdditionalQuery(-1);
            //create streams
            executeAdditionalQuery(1);
            //insert data
            executeAdditionalQuery(2);
            sql = main_select;
        }else{
            sql = sql + "EMIT CHANGES LIMIT 1;";
        }

        // Result
        return sql;
    }

    private void executeAdditionalQuery(int type){
        /*type -1 = drop
        type 0 = select
        type 1 = create
        type 2 = insert*/

        int tmp_counter;

        switch(type) {
            case -1:
                drop_tmpStreams();
                break;
            case 0:
                try{
                    sqlStatement.executeQuery(main_select);
                }catch (SQLException ex){
                    ex.printStackTrace();
                }
                break;
            case 1:
                tmp_counter = create_stream.size()-1;
                while (tmp_counter > -1)
                {
                    try{
                        sqlStatement.executeQuery(create_stream.get(tmp_counter));
                        tmp_counter = tmp_counter - 1;
                    }catch (SQLException ex){
                        ex.printStackTrace();
                    }
                }
                break;
            case 2:
                tmp_counter = insert_stream.size()-2;
                while (tmp_counter > -1)
                {
                    try{
                        sqlStatement.executeQuery(insert_stream.get(tmp_counter));
                        tmp_counter = tmp_counter - 1;
                    }catch (SQLException ex){
                        ex.printStackTrace();
                    }
                }
                break;
            default:
                log.debug("Not (yet) supported execution type");
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
