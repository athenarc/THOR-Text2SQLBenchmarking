package discover.testing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import discover.model.OverloadedTupleList;

public class CSVManager {
    
    public static void init() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv"));
            writer.write("query, #ofKWs, #Mappings, #MappingsPerKW, #ofTupleSets, #ofInitRelations," +
                "#ofCNs, #ofSQLQueries, #ofResults, TotalTime, Time MasterIndex, Time PostProcessor," +
                "Time CandidateNetworkGenerator, Time PlanGenerator, Time PlanExecutor," +
                "Sql-IO (b), CPU Index, CPU CNet Gen, CPU PlanGen, Mem Index (mb), Mem CNet Gen (mb), Plan Gen (mb)\n");
            writer.close();
            writer = new BufferedWriter(new FileWriter("results.out"));
            writer.close();
        }
        catch(IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }
    }


    /**
     * Writes a line to file 
     * 
     * @param line
     */
    public static void toFile(CSVLine line) {        
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv", true));                                
            
            // Calculate total mappings 
            line.totalMappings = 0;
            for (Integer val: line.mappingsPerKW.values())
                line.totalMappings += val;

            String str = line.query + ", " + line.numOfKeyword + ", " + (line.totalMappings/line.numOfKeyword) + ", ";

            // Write mappings per kw
            str += "{";
            for (Map.Entry<String,Integer> entry: line.mappingsPerKW.entrySet()) 
                str += entry.getKey() + " : " + entry.getValue() + " | ";
            str = str.substring(0, str.length() - 2) + "} , ";

            str += line.numOfTupleSets + ", " + line.numOfRelations + ", " + line.cnetworks + ", " + line.sqlQueries + ", " + line.numOfTuples + ", ";

            // Write times
            line.totalTime = 0D;
            String timestr = new String();
            for (Double time: line.timerPerFunction) {
                line.totalTime += time;
                timestr += time + ", ";
            }
            str += line.totalTime + ", " + timestr.substring(0, timestr.length() - 2) + ", ";

            // Write IO-CPU-Mem
            str += line.sqlIO + ", "
                 + line.cpuIndex + ", " + line.cpuCNetGen + ", " + line.cpuPlanGen + "," 
                 + line.memIndex  + ", " + line.memCNetGen + ", " + line.memPlanGen + "\n";
            

            // Write the str in the new file
            writer.write(str);
            writer.close();

            // Write the results 
            BufferedWriter resWriter = new BufferedWriter(new FileWriter("results.out", true));
            str = new String();

            str += line.query + "\n";

            // prune tuples to 10 results
            if (line.tuples.size() > 10)
                line.tuples = line.tuples.subList(0, 10);

            for (OverloadedTupleList tup : line.tuples) {
                if (!tup.isEmpty()) {
                    tup.subList(20);  // prune result to 20
                    str += tup.getTupleList().get(0).getQuery() + "\n" +  tup.toString() + "\n";
                }
            }

            str += "\n\n==================================================\n\n";

            resWriter.write(str);
            resWriter.close();
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }

    }


}