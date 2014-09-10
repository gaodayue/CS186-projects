package simpledb;
import java.util.Map;
import java.util.Vector;
import java.util.HashMap;
import java.io.File;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * LogicalPlan represents a logical query plan that has been through
 * the parser and is ready to be processed by the optimizer.
 * <p>
 * A LogicalPlan consists of a collection of table scan nodes, join
 * nodes, filter nodes, a select list, and a group by field.
 * LogicalPlans can only represent queries with one aggregation field
 * and one group by field.
 * <p>
 * LogicalPlans can be converted to physical (optimized) plans using
 * the {@link #physicalPlan} method, which uses the
 * {@link JoinOptimizer} to order joins optimally and to select the
 * best implementations for joins.
 */
public class LogicalPlan {
    private Vector<LogicalJoinNode> joins;
    private Vector<LogicalScanNode> tables;
    private Vector<LogicalFilterNode> filters;
    private HashMap<String,DbIterator> subplanMap;
    private HashMap<String,Integer> tableMap;

    private Vector<LogicalSelectListNode> selectList;
    private String groupByField = null;
    private boolean hasAgg = false;
    private String aggOp;
    private String aggField;
    private boolean oByAsc, hasOrderBy = false;
    private String oByField;
    private String query;
//    private Query owner;

    /** Constructor -- generate an empty logical plan */
    public LogicalPlan() {
        joins = new Vector<LogicalJoinNode>();
        filters = new Vector<LogicalFilterNode>();
        tables = new Vector<LogicalScanNode>();
        subplanMap = new HashMap<String,DbIterator>();
        tableMap = new HashMap<String,Integer>();

        selectList = new Vector<LogicalSelectListNode>();
        this.query = "";
    }

    /** Set the text of the query representing this logical plan.  Does NOT parse the
        specified query -- this method is just used so that the object can print the
        SQL it represents.

        @param query the text of the query associated with this plan
    */
    public void setQuery(String query)  {
        this.query = query;
    }
      
    /** Get the query text associated with this plan via {@link #setQuery}.
     */
    public String getQuery() {
        return query;
    }

    /** Given a table alias, return id of the table object (this id can be supplied to {@link Catalog#getDbFile(int)}).
        Aliases are added as base tables are added via {@link #addScan}.

        @param alias the table alias to return a table id for
        @return the id of the table corresponding to alias, or null if the alias is unknown
     */
    public Integer getTableId(String alias) {
        return tableMap.get(alias);
    }
    
    public HashMap<String,Integer> getTableAliasToIdMapping()
    {
        return this.tableMap;
    }

    /** Add a new filter to the logical plan
     *   @param field The name of the over which the filter applies;
     *   this can be a fully qualified field (tablename.field or
     *   alias.field), or can be a unique field name without a
     *   tablename qualifier.  If it is an ambiguous name, it will
     *   throw a ParsingException
     *   @param p The predicate for the filter
     *   @param constantValue the constant to compare the predicate
     *   against; if field is an integer field, this should be a
     *   String representing an integer
     *   @throws ParsingException if field is not in one of the tables
     *   added via {@link #addScan} or if field is ambiguous (e.g., two
     *   tables contain a field named field.)
     */
    public void addFilter(String field, Predicate.Op p, String
        constantValue) throws ParsingException {

        field = disambiguateName(field);
        String[] qualifiedName = field.split("[.]");
        if (qualifiedName[1].equals("*"))
            throw new ParsingException("Invalid field '*' in WHERE clause");
        
        LogicalFilterNode lf = new LogicalFilterNode(qualifiedName[0], qualifiedName[1], p, constantValue);
        filters.addElement(lf);
    }

    /** Add a join between two fields of two different tables.  
     *  @param joinField1 The name of the first join field; this can
     *  be a fully qualified name (e.g., tableName.field or
     *  alias.field) or may be an unqualified unique field name.  If
     *  the name is ambiguous or unknown, a ParsingException will be
     *  thrown.
     *  @param joinField2 The name of the second join field
     *  @param pred The join predicate
     *  @throws ParsingException if either of the fields is ambiguous,
     *      or is not in one of the tables added via {@link #addScan}
    */

    public void addJoin( String joinField1, String joinField2, Predicate.Op pred) throws ParsingException {
        String qualifiedName1 = disambiguateName(joinField1);
        String qualifiedName2 = disambiguateName(joinField2);

        String[] field1 = qualifiedName1.split("[.]");
        String[] field2 = qualifiedName2.split("[.]");

        if (field1[1].equals("*") || field2[1].equals("*"))
            throw new ParsingException("Cannot use * as join field");

        if (field1[0].equals(field2[0]))
            throw new ParsingException("Cannot join on two fields from same table");

        LogicalJoinNode lj = new LogicalJoinNode(field1[0], field2[0], field1[1], field2[1], pred);
        System.out.println("Added join between " + qualifiedName1 + " and " + qualifiedName2);
        joins.addElement(lj);

    }

    /** Add a join between a field and a subquery.
     *  @param joinField1 The name of the first join field; this can
     *  be a fully qualified name (e.g., tableName.field or
     *  alias.field) or may be an unqualified unique field name.  If
     *  the name is ambiguous or unknown, a ParsingException will be
     *  thrown.
     *  @param joinField2 the subquery to join with -- the join field
     *    of the subquery is the first field in the result set of the query
     *  @param pred The join predicate.
     *  @throws ParsingException if either of the fields is ambiguous,
     *      or is not in one of the tables added via {@link #addScan}
     */
    public void addJoin( String joinField1, DbIterator joinField2, Predicate.Op pred) throws ParsingException {
        joinField1 = disambiguateName(joinField1);

        String table1 = joinField1.split("[.]")[0];
        String pureField = joinField1.split("[.]")[1];

        LogicalSubplanJoinNode lj = new LogicalSubplanJoinNode(table1,pureField, joinField2, pred);
        System.out.println("Added subplan join on " + joinField1);
        joins.addElement(lj);
    }

    /** Add a scan to the plan. One scan node needs to be added for each alias of a table
        accessed by the plan.
        @param table the id of the table accessed by the plan (can be resolved to a DbFile using {@link Catalog#getDbFile}
        @param alias the alias of the table in the plan
    */
    public void addScan(int table, String alias) {
        System.out.println("Added scan of table " + alias);
        tables.addElement(new LogicalScanNode(table,alias));
        tableMap.put(alias,table);
    }

    /** Add a specified field/aggregate combination to the select list of the query.
        Fields are output by the query such that the rightmost field is the first added via addProjectField.
        @param fname the field to add to the output
        @param aggOp the aggregate operation over the field.
     * @throws ParsingException 
    */
    public void addProjectField(String fname, String aggOp) throws ParsingException {
        String qualifiedName = disambiguateName(fname);
        System.out.println("Added select list field " + qualifiedName);
        if (aggOp != null) {
            System.out.println("\t with aggregator " + aggOp);
        }
        selectList.addElement(new LogicalSelectListNode(aggOp, qualifiedName));
    }
    
    /** Add an aggregate over the field with the specified grouping to
        the query.  SimpleDb only supports a single aggregate
        expression and GROUP BY field.
        @param op the aggregation operator
        @param afield the field to aggregate over
        @param gfield the field to group by
     * @throws ParsingException 
    */
    public void addAggregate(String op, String afield, String gfield) throws ParsingException {
        aggOp = op;
        aggField = disambiguateName(afield);
        groupByField = (gfield == null) ? null : disambiguateName(gfield);
        hasAgg = true;
    }

    /** Add an ORDER BY expression in the specified order on the specified field.  SimpleDb only supports
        a single ORDER BY field.
        @param field the field to order by
        @param asc true if should be ordered in ascending order, false for descending order
     * @throws ParsingException 
    */
    public void addOrderBy(String field, boolean asc) throws ParsingException {
        oByField = disambiguateName(field);
        oByAsc = asc;
        hasOrderBy = true;
        if (oByField.split("[.]")[1].equals("*"))
            throw new ParsingException("ORDER BY * is not supported");
    }

    /** Given a name of a field, try to figure out what table it belongs to by looking
     *   through all of the tables added via {@link #addScan}. 
     *  @return valid "tableAlias.fieldName" or "tableAlias.*" or "null.*"
     *  @throws ParsingException if the field refers to invalid table alias or field name,
     *          or if the field is ambiguous (appears in multiple tables)
     */
    String disambiguateName(String name) throws ParsingException {
        if (name.equals("*") || name.equals("null.*"))
            return "null.*"; // special value means all fields

        String[] fields = name.split("[.]");
        if (fields.length > 2)
            throw new ParsingException("Field " + name + " is not a valid field reference.");

        // if user explicitly specifies tableAlias.fieldName,
        // check for existence of table and field
        if (fields.length == 2 && (!fields[0].equals("null"))) {
            if (!tableMap.containsKey(fields[0]))
                throw new ParsingException("Field " + name + " references an invalid table");
            if (fields[1].equals("*"))
                return name;
            // check existence of field
            TupleDesc td = Database.getCatalog().getTupleDesc(tableMap.get(fields[0]));
            if (!td.hasField(fields[1]))
                throw new ParsingException("Field " + name + " doesn't exist");
            return name;    // valid tableAlias.fieldName
        }

        // either null.fieldName or fieldName, must find the belonging table
        if (fields.length == 2)
            name = fields[1];

        // find alias of table the field belongs to
        String tableAlias = null;
        for (LogicalScanNode scanNode : tables) {
            TupleDesc td = Database.getCatalog().getTupleDesc(scanNode.tableId);
            if (td.hasField(name)) {
                if (tableAlias == null) {
                    tableAlias = scanNode.alias;
                } else {
                    throw new ParsingException("Field " + name + " appears in multiple tables");
                }
            }
        }

        if (tableAlias == null)
            throw new ParsingException("Field " + name + " doesn't exist");

        return tableAlias + "." + name;
    }

    /** Convert the aggregate operator name s into an Aggregator.op operation.
     *  @throws ParsingException if s is not a valid operator name 
     */
    static Aggregator.Op getAggOp(String s) throws ParsingException {
        s = s.toUpperCase();
        if (s.equals("AVG")) return Aggregator.Op.AVG;
        if (s.equals("SUM")) return Aggregator.Op.SUM;
        if (s.equals("COUNT")) return Aggregator.Op.COUNT;
        if (s.equals("MIN")) return Aggregator.Op.MIN;
        if (s.equals("MAX")) return Aggregator.Op.MAX;
        throw new ParsingException("Unknown predicate " + s);
    }

    /** Convert this LogicalPlan into a physicalPlan represented by a {@link DbIterator}.  Attempts to
     *   find the optimal plan by using {@link JoinOptimizer#orderJoins} to order the joins in the plan.
     *  @param t The transaction that the returned DbIterator will run as a part of
     *  @param baseTableStats a HashMap providing a {@link TableStats}
     *    object for each table used in the LogicalPlan.  This should
     *    have one entry for each table referenced by the plan, not one
     *    entry for each table alias (so a table t aliases as t1 and
     *    t2 would have just one entry with key 't' in this HashMap).
     *  @param explain flag indicating whether output visualizing the physical
     *    query plan should be given.
     *  @throws ParsingException if the logical plan is not valid
     *  @return A DbIterator representing this plan.
     */ 
    public DbIterator physicalPlan(TransactionId t, Map<String,TableStats> baseTableStats, boolean explain) throws ParsingException {
        // table alias => selectivity
        // the reason we use alias as key here is that in case like self-join,
        // one table can have several aliases which has different selectivity
        HashMap<String,Double> selectivities = new HashMap<String, Double>();

        // base table name => statistics
        // this is a subset of baseTableStats, which contains only tables in the query
        HashMap<String,TableStats> statsMap = new HashMap<String,TableStats>();

        // init each scan node as a sequence scan operator
        // ---------------------------------------------------
        for (LogicalScanNode scanNode : tables) {
            SeqScan ss = new SeqScan(t, scanNode.tableId, scanNode.alias);
            subplanMap.put(scanNode.alias, ss);
            String baseTableName = Database.getCatalog().getTableName(scanNode.tableId);
            statsMap.put(baseTableName, baseTableStats.get(baseTableName));
            selectivities.put(scanNode.alias, 1.0);
        }

        // add filter operator and estimate table's selectivity
        // ---------------------------------------------------
        for (LogicalFilterNode filterNode : filters) {
            DbIterator childPlan = subplanMap.get(filterNode.tableAlias);
            TupleDesc td = childPlan.getTupleDesc();
            // NOTE: tuple description of scan node uses qualified field name
            int fieldIndex = td.fieldNameToIndex(filterNode.qualifiedFieldName);
            Type fieldType = td.getFieldType(fieldIndex);

            Field constantField = (fieldType == Type.INT_TYPE) ?
                    new IntField(Integer.parseInt(filterNode.c)) :
                    new StringField(filterNode.c, Type.STRING_LEN);

            // add filter operator
            Predicate predicate = new Predicate(fieldIndex, filterNode.p, constantField);
            subplanMap.put(filterNode.tableAlias, new Filter(predicate, childPlan));

            // update selectivity
            String originalName = Database.getCatalog().getTableName(this.getTableId(filterNode.tableAlias));
            TableStats stats = statsMap.get(originalName);
            double oldSel = selectivities.get(filterNode.tableAlias);
            selectivities.put(filterNode.tableAlias, oldSel * stats.estimateSelectivity(predicate));
        }

        // order joins, pick the best physical plan
        // ---------------------------------------------------
        JoinOptimizer optimizer = new JoinOptimizer(this, joins);
        joins = optimizer.orderJoins(statsMap, selectivities, explain);


        // build up plan according to the join order
        // ---------------------------------------------------
        // NOTE that we consider all linear plan rather than left-deep plan in optimizer
        // and joins[0] is the bottom-most join.

        // equivMap[t2] == t1 means t1 & t2 have been joined together and
        // you should always use subplanMap[t1] to get the plan
        HashMap<String,String> equivMap = new HashMap<String,String>();

        for (LogicalJoinNode joinNode : joins) {
            boolean isSubQueryJoin = joinNode instanceof LogicalSubplanJoinNode;

            DbIterator leftPlan;
            DbIterator rightPlan;
            String leftKey = null;
            String rightKey = null;

            if (equivMap.containsKey(joinNode.t1Alias)) {
                leftKey = equivMap.get(joinNode.t1Alias);
            } else {
                leftKey = joinNode.t1Alias;
            }
            leftPlan = subplanMap.get(leftKey);

            if (isSubQueryJoin) {
                rightPlan = ((LogicalSubplanJoinNode) joinNode).subPlan;
                if (rightPlan == null) throw new ParsingException("invalid sub query");

            } else {
                if (equivMap.containsKey(joinNode.t2Alias)) {
                    rightKey = equivMap.get(joinNode.t2Alias);
                } else {
                    rightKey = joinNode.t2Alias;
                }
                rightPlan = subplanMap.get(rightKey);
            }

            assert leftPlan != null && rightPlan != null;
            subplanMap.put(leftKey, JoinOptimizer.instantiateJoin(joinNode, leftPlan, rightPlan));

            if (!isSubQueryJoin) {
                subplanMap.remove(rightKey);
                // all entries in equivMap should point to leftKey, which
                // identified the current joined tree
                equivMap.put(rightKey, leftKey);
                for (Map.Entry<String, String> entry : equivMap.entrySet()) {
                    // old entries pointed to rightKey should point to leftKey now
                    if (entry.getValue().equals(rightKey)) {
                        entry.setValue(leftKey);
                    }
                }
            }
        }

        if (subplanMap.size() > 1)
            throw new ParsingException("Query does not include join expressions joining all nodes!");
        
        DbIterator node =  subplanMap.entrySet().iterator().next().getValue();
        TupleDesc nodeTd = node.getTupleDesc();

        // project output fields in select list
        // ---------------------------------------------------
        ArrayList<Integer> outFields = new ArrayList<Integer>();
        ArrayList<Type> outTypes = new ArrayList<Type>();

        // special case: if using aggregation, the result is either
        // 1 column (aggregation result) or 2 columns (group name and aggregation value)
        if (hasAgg) {
            int idx = 0;
            if (groupByField != null) {
                if (!groupByField.equals(selectList.get(0).qualifiedFieldName))
                    throw new ParsingException("the first field in select list is not the group by field");
                // for group name field
                outFields.add(idx++);
                outTypes.add(nodeTd.getFieldType(nodeTd.fieldNameToIndex(groupByField)));
            }

            if (selectList.size() != idx + 1)
                throw new ParsingException("with aggregation, there should be " + (idx + 1) + " fields in select list");
            if (selectList.get(idx).aggOp == null)
                throw new ParsingException("with aggregation, the " + (idx + 1) + " field should be an aggregation");

            // for aggregation field
            outFields.add(idx);
            outTypes.add(Type.INT_TYPE);  //the type of all aggregate functions is INT

        } else {
            for (LogicalSelectListNode selectNode : selectList) {
                if (selectNode.qualifiedFieldName.equals("null.*")) {
                    // project all fields
                    for (int j = 0; j < nodeTd.numFields(); j++) {
                        outFields.add(j);
                        outTypes.add(nodeTd.getFieldType(j));
                    }

                } else {
                    // project one field
                    int fieldIndex = nodeTd.fieldNameToIndex(selectNode.qualifiedFieldName);
                    outFields.add(fieldIndex);
                    outTypes.add(nodeTd.getFieldType(fieldIndex));
                }
            }
        }

        // add aggregation operator
        // ---------------------------------------------------
        if (hasAgg) {
            int aggFieldIndex = nodeTd.fieldNameToIndex(aggField);
            int gbFieldIndex = groupByField == null ?
                                    Aggregator.NO_GROUPING :
                                    nodeTd.fieldNameToIndex(groupByField);

            node = new Aggregate(node, aggFieldIndex, gbFieldIndex, getAggOp(aggOp));
        }

        // add order by operator
        // ---------------------------------------------------
        if (hasOrderBy) {
            node = new OrderBy(nodeTd.fieldNameToIndex(oByField), oByAsc, node);
        }

        return new Project(outFields, outTypes, node);
    }

    public static void main(String argv[]) {
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };

        TupleDesc td = new TupleDesc(types, names);
        TableStats ts;
        HashMap<String, TableStats> tableMap = new HashMap<String,TableStats>();

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");
        ts = new TableStats(table1.getId(), 1);
        tableMap.put("t1", ts);

        TransactionId tid = new TransactionId();

        LogicalPlan lp = new LogicalPlan();
        
        lp.addScan(table1.getId(), "t1");

        try {
            lp.addFilter("t1.field0", Predicate.Op.GREATER_THAN, "1");
        } catch (Exception e) {
        }

        /*
        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                                new Predicate(0,
                                Predicate.Op.GREATER_THAN, new IntField(1)),  ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);
        */
        DbIterator j = null;
        try {
            j = lp.physicalPlan(tid,tableMap, false);
        } catch (ParsingException e) {
            e.printStackTrace();
            System.exit(0);
        }
        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }
       
    }

}