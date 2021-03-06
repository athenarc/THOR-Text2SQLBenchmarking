package discoverIR.testing;

import java.util.ArrayList;
import java.util.List;

import discoverIR.model.OverloadedTupleList;

public class CSVLine {
    String query;

// 1) #KWs
    public Integer numOfKeyword;

// 2) #total mappings for a query
    public Integer totalMappings;

    public Integer numOfTupleSets;

// 3) # Initial Relations for a query
    public Integer numOfRelations;


    public Integer cnetworks;

// 5) #SQL queries to execute
    public Integer sqlQueries;

// 6) #final tuples
    public Integer numOfTuples;

// 7) the final tuples
    public List<OverloadedTupleList> tuples = new ArrayList<>();

// 8) execution time in each function
    public List<Double> timerPerFunction = new ArrayList<>();

// 9) total execution time
    public Double totalTime;

// 10 SizeofSQL IO
    public long sqlIO;

// 11-13 CPU 
    public double cpuIndex;
    public double cpuCNetGen;

// 13-15 Mem 
    public double memIndex;
    public double memCNetGen;
}