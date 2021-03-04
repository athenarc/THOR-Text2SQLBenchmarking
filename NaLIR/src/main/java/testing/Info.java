package testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;


public class Info {

    public String query;

    public Integer totalMappings;
    public HashMap<String, SimpleEntry<Integer, Integer>> mappingsPerKW = new HashMap<>();

    public Integer numOfIRepresentations;

    public List<Double> timerPerComponent = new ArrayList<>();

    public Double totalTime;

    public Integer numOfResults;

// 10 SizeofSQL IO
    public long sqlIO;

// 11-13 CPU 
    public double cpuParser;
    public double cpuMapper;
    public double cpuEntRes;    
    public double cpuTranslator;    

// 13-15 Mem 
    public double memParser;
    public double memMapper;
    public double memEntRes;
    public double memTranslator;
    
}


