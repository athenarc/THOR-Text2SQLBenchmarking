package expressq2.components;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import shared.database.connectivity.DatabaseUtil;
import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLForeignKeyConstraint;
import shared.database.model.SQLQueries;
import shared.database.model.SQLTable;
import shared.database.model.graph.ORMNode;
import shared.database.model.graph.ORMSchemaGraph;
import expressq2.model.OperationAnnotation;
import expressq2.model.ComponentRelationAnnotation;
import expressq2.model.ConditionAnnotation;
import expressq2.model.PatternNode;
import expressq2.model.QueryPattern;
import expressq2.model.SQLQuery;
import shared.util.Pair;

/**
 * Input: A QueryPattern P Output: An SQL Query ready to be executed by the
 * underling SQLDatabase.
 *
 */
public class QueryTranslator {


    /**
     * Translates a queryPattern into an SQL Query in string format.
     *
     * @param queryPattern
     * @param schemaGraph
     * @return
     */
    public static SQLQuery translateQueryPattern(SQLDatabase database, QueryPattern queryPattern, ORMSchemaGraph schemaGraph) {
        SQLQuery query = new SQLQuery();
        HashMap<PatternNode, String> patternNodesWithAliases = new HashMap<>();  // A map storing patterNodes and Aliases.

        // Map each QueryPatterNode with one Alias for the SQL Query
        HashMap<Character, Integer> genAliases = new HashMap<>();
        for (PatternNode node: queryPattern.getVertexes()) {
            query.addTable(node.getReferredRelationName()); // Keep the Relation for the SQLQuery.

            // Create an alias based on the first letter of the relation and a number of times that letter was used.
            Character firstLetter = node.getReferredRelationName().charAt(0);
            Integer i = genAliases.getOrDefault(firstLetter, 0);
            String alias =  firstLetter + i.toString() ;

            // Update the alias Generator mapping.
            genAliases.put(firstLetter, i + 1);

            // Put the mapping in the hashMap.
            patternNodesWithAliases.put(node, alias);
        }

        // First generate the Select Clause of the Query. Handle the two different cases of
        // select clause generation based on the the Queries nature (aggregate or non aggregate).
        String selectClause = null;
        if (queryPattern.isAggregateQuery()) {
            selectClause = generateSelectClauseAgg(patternNodesWithAliases, schemaGraph);
            // System.out.println("SELECT Clause " + selectClause);
        }
        else
            selectClause = generateSelectClauseNonAgg(patternNodesWithAliases, schemaGraph);

        // Then generate the From Clause of the Query.
        String fromClause = generateFromClause(queryPattern, patternNodesWithAliases, schemaGraph);

        // Then generate the Where Clause of the Query.
        String whereClause = generateWhereClause(database, queryPattern, patternNodesWithAliases, schemaGraph);

        // And lastly the GroupBy Clause of the Query.
        String groupByClause = new String();
        if (queryPattern.isAggregateQuery())
            groupByClause = generateGroupByClause(patternNodesWithAliases, schemaGraph);

        // The SQL Select Query.
        String sqlQuery = String.format(
            SQLQueries.SQL_SELECT_QUERY,
                selectClause + "\n",
                fromClause +
                ((whereClause.isEmpty())   ?  "" : "\n" + String.format(SQLQueries.SQL_WHERE_CLAUSE, whereClause)) +
                ((groupByClause.isEmpty()) ?  "" : "\n" + String.format(SQLQueries.SQL_GROUPBY_CLAUSE, groupByClause))
        );

        // Handle the Case that the query is nested.
        if (queryPattern.isNested())
            sqlQuery = generateNestedQuery(queryPattern.getNestedAggregateAnnotation(), sqlQuery);


        query.setQuery(sqlQuery);
        return query;
    }


    /**
     * Generated the Select Clause of the query for aggregate Queries:
     *  1) If a node u is annotated with t(a) and t matches an aggregate function,
     * we include t in the SELECT clause, and t is applied on attribute a.
     *  2) If u is annotated with GROUPBY(a), we include a in the SELECT clause to
     * facilitate user understanding of the aggregate function(s).
     *
     * @param patternNodesWithAliases the nodes of the QueryPattern to examine along with their aliases.
     * @param schemaGraph the SchemaGraph of the database.
     * @return the select Clause to string.
     */
    public static String generateSelectClauseAgg (
      HashMap<PatternNode, String> patternNodesWithAliases,
      ORMSchemaGraph schemaGraph)
    {
        String selectClause = new String();  // The Select Clause.

        // Loop all the Nodes of the QueryPattern.
        for (Map.Entry<PatternNode, String> nodeAliasPair: patternNodesWithAliases.entrySet()) {
            PatternNode node = nodeAliasPair.getKey();     // The PatterNode.
            String nodeAlias = nodeAliasPair.getValue();   // The Alias Assigned.

            // Examine all the node's AggregateAnnotations.
            for (OperationAnnotation annotation: node.getOperationAnnotations()) {
                // if the annotation contains a groupBy Term then add the column to the select clause.
                if (annotation.getOperator().isGroupByFunction()) {
                    selectClause += nodeAlias + "." + annotation.getColumn() + ", ";
                }
                // Else if the annotation contains an Aggregation Function Term then add
                // the aggregation applied to the column.
                else if (annotation.getOperator().isAggregateFunction() ) {
                    selectClause += annotation.getOperator().getTerm() +
                                    "(" +  nodeAlias + "." + annotation.getColumn() + ")" +
                                    SQLQueries.ALIAS_SPECIFIER + annotation.getAlias() + ", ";
                }
            }
        }

        // Remove the last ", " if selectClause is not empty
        return (selectClause.isEmpty()) ? selectClause : selectClause.substring(0, selectClause.length() - 2);
    }


    /**
     * Generated the Select Clause of the query for aggregate Queries :
     *  1) If node u is a target node and speciﬁes a search target via the object or relationship name
     * then we include all the attributes of the relations of u in the SELECT clause.
     *  2) If u is a target node and speciﬁes a search target via an attribute name then we include
     *  only the corresponding attribute of the relations of u in the SELECT clause.
     *
     * @param patternNodesWithAliases the nodes of the QueryPattern to examine along with their aliases.
     * @param schemaGraph the SchemaGraph of the database.
     * @return the select Clause to string.
     */
    public static String generateSelectClauseNonAgg (
      HashMap<PatternNode, String> patternNodesWithAliases,
      ORMSchemaGraph schemaGraph)
    {
        String selectClause = new String();  // The Select Clause.

        // Loop all the Nodes of the QueryPattern.
        for (Map.Entry<PatternNode, String> nodeAliasPair: patternNodesWithAliases.entrySet()) {
            PatternNode node = nodeAliasPair.getKey();     // The PatterNode.
            String nodeAlias = nodeAliasPair.getValue();   // The Alias Assigned.

            // Case 1
            if (node.isTargetNode() && node.getTagGrouping().refersToRelation()) {
                // Add all attributes of the nodes referred relation in the select clause.
                for (SQLColumn column: node.getSchemaNode().getRelation().getColumns()){
                    selectClause += nodeAlias + "." + column.getName() + ", ";
                }
            }

            // Case 2
            if (node.isTargetNode() && node.getTagGrouping().refersToAttribute()) {
                // Add the specific attribute in the select clause
                selectClause += nodeAlias + "." + node.getTagGrouping().getReferredAttribute() + ", ";
            }
        }

        // Remove the last ", " if selectClause is not empty.
        // If select clause is empty then just select everything.
        return (selectClause.isEmpty()) ? "*" : selectClause.substring(0, selectClause.length() - 2);
    }



    /**
     * Generated the From Clause of the query :
     * Add all the relations referred by the PatternNodes of the QueryPattern. In case
     * a node u refers to a relationship relation r, compare the number of Nodes connected
     * with u and the number of nodes connected with r in the ORMSchemaGraph. If nodes
     * connected with u are less than node connected with r create a sql query projecting
     * only the relations referred by the nodes connected with u to avoid duplicates.
     *
     * @param queryPattern the queryPattern we are translating.
     * @param patternNodesWithAliases the nodes of the QueryPattern to examine along with their aliases.
     * @param schemaGraph the SchemaGraph of the database.
     * @return the from Clause to string.
     */
    public static String generateFromClause(
      QueryPattern queryPattern,
      HashMap<PatternNode, String> patternNodesWithAliases,
      ORMSchemaGraph schemaGraph
    ) {
        String fromClause = new String();  // The From Clause.

        // Loop all the Nodes of the QueryPattern.
        for (Map.Entry<PatternNode, String> nodeAliasPair: patternNodesWithAliases.entrySet()) {
            PatternNode node = nodeAliasPair.getKey();   // The PatterNode.
            String alias = nodeAliasPair.getValue();     // The Alias Assigned.

            // ===========================================
            // Commented it because it takes too much time
            // ===========================================

            // If node refers to a Relationship Node then create a
            // nested SQLQuery to avoid duplicates if needed.
            // if (node.getSchemaNode().isRelationshipNode()) {
            //     List<PatternNode> patternNeighbors = queryPattern.getNodesNeighbors(node);           // The relationship nodes neighbors in the QueryPattern.
            //     List<ORMNode> schemaNeighbors = schemaGraph.getNodesNeighbors(node.getSchemaNode()); // The relationship nodes neighbors in the SchemaGraph.

            //     // Filter the patternNeighbors to PatternNodes that dont refer to the same Relation.
            //     HashMap<String, PatternNode> filteredPatterNodes = new HashMap<>();
            //     for (PatternNode patternNode: patternNeighbors)
            //         filteredPatterNodes.putIfAbsent(patternNode.getReferredRelationName(), patternNode);
            //     patternNeighbors.clear();
            //     patternNeighbors.addAll(filteredPatterNodes.values());


            //     // If the nodes connected with the relationshipNode in the QueryPattern are less than
            //     // the nodes in the schemaGraph then it means we have to project that nodes in the
            //     // relationship node with a select Distinct Query to avoid duplicates.
            //     if (patternNeighbors.size() < schemaNeighbors.size())
            //         fromClause += "(" + generateProjectionQuery(node, patternNeighbors, schemaGraph) +
            //                       ")" + SQLQueries.ALIAS_SPECIFIER + alias + ", ";
            //     else
            //         fromClause += node.getReferredRelationName() +  SQLQueries.ALIAS_SPECIFIER + alias + ", ";
            // }
            // If the PatterNode contains a ComponentRelation Annotation this means that we need to create
            // a nested query connecting the Object node corresponding to the PatterNode with the
            // component Relation of this node.
            // else if (node.containsComponentRelationAnnotation()) {
            if (node.containsComponentRelationAnnotation()) {
                fromClause += "(" + generateQueryConnectingObjectWithComponentRelations(
                                        node.getSchemaNode(), node.getComponentRelationAnnotations()
                                    ) +
                              ")" + SQLQueries.ALIAS_SPECIFIER + alias + ", ";
            }
            // Else node refers to Object/Mixed node so simply add the relation along with its alias to the query.
            else {
                fromClause += node.getReferredRelationName() +  SQLQueries.ALIAS_SPECIFIER + alias + ", ";
            }
        }

        // Remove the last ", " if fromClause is empty
        return (fromClause.isEmpty()) ? fromClause : fromClause.substring(0, fromClause.length() - 2);
    }


    /**
     * Generated the Where Clause of the query :
     *  - The WHERE clause joins all the relations in the FROM clause based on
     * foreign key - primary key references. Create conditions joining the nodes.
     *  - For each patterNode that is annotated with a condition a = t, include
     * the condition "r.a contains t" where r is the relation referred by the patterNode.
     *
     * @param queryPattern the query pattern to traverse and extract the join conditions.
     * @param patternNodeAliases a map between patterNodes and their aliases.
     * @param schemaGraph the SchemaGraph of the database.
     * @return the from where Clause to string.
     */
    public static String generateWhereClause (
      SQLDatabase database, QueryPattern queryPattern,
      HashMap<PatternNode, String> patternNodeAliases,
      ORMSchemaGraph schemaGraph )
    {
        String whereClause = new String();  // The Select Clause.
        Queue<PatternNode> queue = new LinkedList<>();    // A queue for the bfs traversal of the QueryPattern.
        Set<PatternNode> visitedNodes = new HashSet<>();  // A set storing visited nodes as we traverse the Pattern.

        // Initialize the queue with a random node.
        queue.add(queryPattern.getVertexes().get(0));

        // BFS Traversal of the QueryPattern.
        while (!queue.isEmpty()) {
            PatternNode fatherNode = queue.poll();                        // The father Node.
            String fatherNodeAlias = patternNodeAliases.get(fatherNode);  // The alias of the above node.

            // First Get the nodes annotations.
            for (ConditionAnnotation annotation: fatherNode.getConditionAnnotations()) {
                if (annotation.getAttribute().isIndexed()) {
                    whereClause += database.getInvIndexCondition()
                    .setColumn(annotation.getAttribute().getName(), fatherNodeAlias)
                    .setSearchPhrase(database.prepareForAndFullTextSearch(annotation.getValue()))
                    .build()
                       +
                    SQLQueries.ADD_WHERE_STMT;
                }
                else {
                    whereClause += fatherNodeAlias + "." + annotation.getAttribute().getName() +
                     " LIKE '%"+ database.prepareForAndFullTextSearch(annotation.getValue()) + "%'" +
                     SQLQueries.ADD_WHERE_STMT;
                }
            }

            // Get all the node's children.
            for (PatternNode childNode: queryPattern.getNodesNeighbors(fatherNode)) {
                // If the node has been visited again skip it to avoid adding duplicate joinConditions.
                if (visitedNodes.contains(childNode)) continue;

                String childNodeAlias = patternNodeAliases.get(childNode); // The alias of the childNode.

                // Get the columns involving in joining father and child node.
                Pair<SQLColumn, SQLColumn> columnsJoiningNodes =
                    schemaGraph.getColumnsJoiningNodes(fatherNode.getSchemaNode(), childNode.getSchemaNode());
                // TODO Handle Exception

                // Add the join condition in the Query.
                whereClause +=
                    fatherNodeAlias + "." + columnsJoiningNodes.getLeft().getName()  + " = " +
                    childNodeAlias  + "." + columnsJoiningNodes.getRight().getName() + SQLQueries.ADD_WHERE_STMT;

                // Add the node to the Queue.
                queue.add(childNode);
            }

            // Set the node as visited.
            visitedNodes.add(fatherNode);
        }

        // Remove the last SQLQueries.ADD_WHERE_STMT if whereClause is empty
        return (whereClause.isEmpty()) ? whereClause : whereClause.substring(0, whereClause.length() - SQLQueries.ADD_WHERE_STMT.length());
    }


    /**
     * Generated the GroupBy Clause of the query :
     *  - If u is annotated with GROUPBY(a), we include a in the GroupBy Clause.
     *
     * @param patternNodesWithAliases the nodes of the QueryPattern to examine along with their aliases.
     * @param query The SQLQuery whose Select Clause we are going to Generate.
     * @param schemaGraph the SchemaGraph of the database.
     * @return the from GroupBy to string.
     */
    public static String generateGroupByClause (
        HashMap<PatternNode, String> patternNodesWithAliases,
        ORMSchemaGraph schemaGraph)
    {
        String groupByClause = new String();  // The GroupBy Clause.

        // Loop all the Nodes of the QueryPattern.
        for (Map.Entry<PatternNode, String> nodeAliasPair: patternNodesWithAliases.entrySet()) {
            PatternNode node = nodeAliasPair.getKey();   // The PatterNode.
            String alias = nodeAliasPair.getValue();     // The Alias Assigned.

            // Examine all the node's AggregateAnnotations.
            for (OperationAnnotation annotation: node.getOperationAnnotations()) {
                // if the annotation contains a groupBy Term then add the column to the query.
                if (annotation.getOperator().isGroupByFunction()) {
                    groupByClause += alias + "." + annotation.getColumn() + ", ";
                    break;
                }
            }
        }

        // Remove the last " AND " if groupByClause is empty
        return (groupByClause.isEmpty()) ? groupByClause : groupByClause.substring(0, groupByClause.length() - 2);
    }


    /**
     * Create a query projecting to the relation referred by the relationshipNode
     * the relations referred by the nodesToProject Nodes. The query will be a
     *
     * @param relationshipNode
     * @param nodesToProject
     * @param schemaGraph
     * @return
     */
    public static String generateProjectionQuery(
      PatternNode relationshipNode,
      List<PatternNode> nodesToProject,
      ORMSchemaGraph schemaGraph
    ) {
        String selectClause = new String();    // The select clause of the query.
        String fromClause = new String();      // The from clause of the query.
        Set<String> columns = new HashSet<>(); // Stores distinct columnNames.

        // If the node contains any annotations then add their column name's in the projection query.
        for (OperationAnnotation annotation: relationshipNode.getOperationAnnotations())
            columns.add(annotation.getColumn());
        for (ConditionAnnotation annotation: relationshipNode.getConditionAnnotations())
            columns.add(annotation.getAttribute().getName());


        // For each nodeToProject get their column joining with this relationshipNode
        // and project them to the relation corresponding to relationshipNode.
        for (PatternNode node: nodesToProject) {
            // Get the columns involving in joining father and child node.
            Pair<SQLColumn, SQLColumn> columnsJoiningNodes =
                schemaGraph.getColumnsJoiningNodes(relationshipNode.getSchemaNode(), node.getSchemaNode());

            // Add the relationshipNode's column in the select clause.
            columns.add(columnsJoiningNodes.getLeft().getName());
        }

        // Create the Select Clause.
        for (String column: columns)
            selectClause += column + ", ";
        selectClause = selectClause.substring(0, selectClause.length() - 2);

        // Add the relation corresponding to relationshipNode in the from Clause.
        fromClause += relationshipNode.getReferredRelationName();

        // Return the query.
        return String.format(
                SQLQueries.SQL_SELECT_DISTINCT_QUERY,
                selectClause + "\n",
                fromClause
            );
    }


      /**
     * Generate an SQL Query, in string format, connecting the Object/Mixed node corresponding
     * to the parameter node with all its component Relations given by the set of ComponentRelationAnnotations.
     *
     * @param node the object/Mixed node.
     * @param ComponentRelationAnnotations A set of annotations providing the function with the component Relations Names.
     * @return the sql query in string format.
     */
    private static String generateQueryConnectingObjectWithComponentRelations(
      ORMNode node,
      Set<ComponentRelationAnnotation> ComponentRelationAnnotations)
    {
        String selectClause = new String();        // The select clause of the query.
        String fromClause = new String();          // The from clause of the query.
        String whereClause = new String();         // The where clause of the query.
        SQLTable objectRel = node.getRelation();   // The object relation corresponding to the node.

        // Add the Object Relation in the from clause.
        fromClause += objectRel.getName() + ", ";

        // Add all the columns of the object relation in the select clause.
        for (SQLColumn column: objectRel.getColumns())
            selectClause += objectRel.getName() + "." + column.getName() + ", ";

        // Loop every Component Relation Annotation.
        for (ComponentRelationAnnotation annotation: ComponentRelationAnnotations) {
            // Get the Pair <Table, Constraint> for the component relation of the node.
            Pair<SQLTable, SQLForeignKeyConstraint> pair = node.getComponentRelationByName(annotation.getComponentRelation());
            SQLForeignKeyConstraint constraint = pair.getRight();
            SQLTable componentRel = pair.getLeft();

            // Get a pair of the columns involving in joining those two relations.
            Pair<SQLColumn, SQLColumn> joiningColumns = constraint.getColumnPairIfTablesPatricipateInConstraint(
                objectRel, componentRel, null
            );

            // Add the table in the from clause.
            fromClause += componentRel.getName() + ", ";

            // Add all the columns in the select Clause except the Joining Column.
            for (SQLColumn column: componentRel.getColumns())
                if (!column.equals(joiningColumns.getRight()))
                    selectClause += componentRel.getName() + "." + column.getName() + ", ";

            // Fill the Where clause with the equality joining the two tables
            whereClause += componentRel.getName() + "." + joiningColumns.getRight().getName() +  " = " +
                           objectRel.getName()    + "." + joiningColumns.getLeft().getName()  + SQLQueries.ADD_WHERE_STMT;
        }

        // Remove the last ", " and " AND " from the select/from/where clauses
        fromClause = fromClause.substring(0, fromClause.length() - 2);
        selectClause = selectClause.substring(0, selectClause.length() - 2);
        whereClause = whereClause.substring(0, whereClause.length() - SQLQueries.ADD_WHERE_STMT.length());

        // Return the query.
        return String.format(
            SQLQueries.SQL_SELECT_QUERY,
                selectClause + "\n",
                fromClause +
                "\n" + String.format(SQLQueries.SQL_WHERE_CLAUSE, whereClause)
        );
    }



    /**
     * Generate a nested query for the QueryPattern. The nesting level is 1 and the outer Query
     * is a select query with an aggregate function.
     *
     * @param nestedAnnotation The aggregation creating the nesting in the query.
     * @param innerQueryToSQL The inner SQLQuery upon with the Aggregation is applied
     * @param fillColumnsToProject  A list to fill with columns in the selected clause of the final query.
     * @return
     */
    public static String generateNestedQuery(
      OperationAnnotation nestedAnnotation,
      String innerQueryToSQL)
    {
        String selectClause = new String();  // The select clause of the outer Query.
        String fromClause = new String();    // The from clause of the outer Query.
        String alias = "N";                  // An alias for the inner Query.

        // Generate the select clause.
        selectClause += nestedAnnotation.getOperator() +
                       "(" + alias + "." + nestedAnnotation.getColumn() + ") " +
                       SQLQueries.ALIAS_SPECIFIER + nestedAnnotation.getAlias();

        fromClause += "(" + innerQueryToSQL + ")" + SQLQueries.ALIAS_SPECIFIER + alias;

        return String.format(
            SQLQueries.SQL_SELECT_QUERY,
                selectClause + "\n",
                fromClause
        );
    }

}