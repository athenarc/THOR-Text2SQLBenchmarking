package expressq2.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;


public class Info {

    public String query;

    public int keywords;

    public int totalMappings;

    public HashMap<String, SimpleEntry<Integer, Integer>> mappingsPerKW = new HashMap<>();

    public Integer numOfAnnotatedQueries;
    public Integer numOfQueryPatters;

    public List<Double> timerPerComponent = new ArrayList<>();

    public Double totalTime;

    public Integer numOfResults;

// 10 SizeofSQL IO
    public long sqlIO;

// 11-13 CPU 
    public double cpuQueryAn;
    public double cpuQueryInter;
    public double cpuQueryTrans;

// 13-15 Mem 
    public double memQueryAn;
    public double memQueryInter;
    public double memQueryTrans;
}


