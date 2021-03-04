package spark.testing;

import java.util.ArrayList;
import java.util.List;

import spark.model.OverloadedTuple;


public class CSVLine {

    public String query;

// 1) #KWs
    public Integer numOfKeyword;

// 2) #total mappings for a query
    public Integer totalMappings;

// 3) # Initial Relations for a query
    public Integer numOfRelations;
    public Integer numOfTupleSets;

// 4) #Iinitial Subgarphs for a query
    public Integer numOfCandidateNetworks;

// 5) #SQL queries to execute
    public Integer sqlQueries;

// 6) #final tuples
    public Integer numOfTuples;

// 8) execution time in each function
    public List<Double> timerPerFunction = new ArrayList<>();

// 9) total execution time
    public Double totalTime;

    public List<OverloadedTuple> topRes = new ArrayList<>();


// 10 SizeofSQL IO
public long sqlIO;

// 11-13 CPU 
    public double cpuIndex;
    public double cpuCNetGen;    

// 13-15 Mem 
    public double memIndex;
    public double memCNetGen;    

}