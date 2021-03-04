# ThorSharedLib

This library provides classes and interfaces that myst be imported or implemented by systems wanting to connect with THOR interface. It also contains an SQL utility used to load any SQL Database to Java Programmes and classes like Graph, Tree, Pair, Ordered List, ... .


# Minimal Example

We will demonstrate how to use `ThorSharedLib` in your DatabaseSearch system's flow in 3 stages:
 * Input Handling
 * Component Handling
 * Output Handling


## Input Handling
We suppose that your System will run on an endless loop waiting for *queries* to execute

```java
// Instantiate Thor's input Handler
InputHandler inputHandler = new InputHandler();

// Loop till the condition brakes from inside the loop
while(true) {            
    
    // Call Thor's read input
    inputHandler.readInput();    
    
    // Read the parameters needed for the execution from the stdin.            
    discoverApp.query = inputHandler.getQuery();
    discoverApp.schemaName = inputHandler.getSchemaName();
    discoverApp.maxResults = inputHandler.getResultsNumber();
    if (inputHandler.shutDownSystem())
        break; 
```
When calling `inputHandler.readInput()` your system will block waiting for THOR to send 3 variables:
 * The *query* to execute.
 * The *schemaName* of the database upon which the query will get executed.
 * The *maximum number of results* the user asked for in the FrontEnd (One result is one Tuple).
 * Keep in mind that `inputHandler.shutdownSystem` will return true if thor is finalizing and wants to shut down 
your system. Generally, THOR might also send a SIGKILL signal if immediate shutdown is ordered by the user, so a signal handler implementation will be useful. 

## Component Handling
A Component is a class of a DatabaseSearch system that handles a specific task in the NL-2-SQL pipeline. 
THOR tries to gather information about those components for 2 reasons:
 * Compare them against other system components.
 * Give the user insights about the system's execution.

To handle components we are using a class named `Component` that stores information like:
 * Component execution time (We provide `startTimer()` / `stopTimer()` methods).
 * Specific information about the component in table-like format (using a Table class provided by us).


We suppose that the system in our example has 2 components, so we create 2 THOR components.
```java
    // Instantiate THOR components and GeneralArchitecture
    Component c1 = new Component("Component 1");
    Component c2 = new Component("Component 2");

    // ---
    c1.startTimer(); // Start timing component 1

    // Here goes the code for the actual Component of the system ...
    ActualComponent1 actualComponent1 = new ActualComponent1();
    actualComponent1.doTask( ... );

    c1.stopTimer(); // Stop timing component 1
    
    // Add Statistics using a function defined by the programmer to return a Table of component's information.
    c1.addComponentInfo( actualComponent1.getStatistics() );
    // ---

    // ...
    // Do the same for component2 of the DatabaseSearch system.
    // ...
```

An implementation of the above `getStatistics()` method could be

```java
public class ActualComponent1 {
    //....

    // Returns a Table filled with statistics from the component
    public Table getStatistics() {
        List<String> columnTitles = new ArrayList<>();   // The column titles.
        List<Table.Row> rows = new ArrayList<>();        // The table rows.

        // Add the column Titles
        columnTitles.addAll(Arrays.asList("C1", "C2", "...", "CN"));  

        // Add rows
        rows.add( new Table.Row( Arrays.asList("Row1.1", "Row1.2", "...", "Row1.N" )) );
        rows.add( new Table.Row( Arrays.asList("Row2.1", "Row2.2", "...", "Row2.N" )) );

        // Return the table
        return new Table("TableName", columnTitles, rows);
    }
}
```


## Output Handling
At the end of each loop the DatabaseSearch system must create a `Response` object and 
submit it to THOR with the following way:
 * First, link the components of your DatabaseSystem together to represent your system's pipeline.
 * Then, create the `Response` object parametrized with your System's class that represents a result (lets name that  `SystemResult` for now). The `SystemResult` is a class representing a **single** result (e.g. An SQL row).

```java
    // .....

    // Create the connections between Components and assign labels.
    c1.connectWith(c2, "Label1");
            
    // Create a response and send it to THOR
    Response<SystemResult> systemResponse = new Response<>(
        "jarName",              // used to run the jar.
        "systemName",           // Displayed on THOR
        Arrays.asList(c1, c2),  // The components
        results                 // The results.
    );
    systemResponse.sendToTHOR();

}  // Here the while loop closes (see InputHandling)
```

Note that your System's result class (in this case the `SystemResult` class) must implement our `ResultInterface` methods:
```java
    /**
     * A list that contains the <Column, Value> pairs of the result (tuple).
     * 
     * @Note The `ColumnValuePair` class is provided by us (It's a simple String Pair).
     */
    public Collection<ColumnValuePair> getColumnValuePairs();

    /**
	 * A list with the names of the tables that were joined to produce the result.
     */
    public Collection<String> getNetworks();

    /**
     * The SQL query that was executed to produce the result.
     */
    public String getQuery();

    /**
     * Returns true if the system assigns a score to every result.
     */
    public boolean hasScoreField();

    /**
     * Returns the result's score if one is present.
     */
    public double getResultScore();
```


