package discoverIR.testing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import discoverIR.model.OverloadedTupleList;

public class CSVManager {

    public static void init() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv"));            
            writer.write("query, #ofKWs, #Mappings, #ofTupleSets, #ofInitRelations," + 
                        " #ofCandidateNetworks, #ofSQLQueries, #ofResults, TotalTime," +
                        " Time IREngine, Time CandidateNetworkGenerator, Time Executor," +
                        " Sql-IO (b), CPU Index, CPU CNet Gen, Mem Index (mb), Mem CNet Gen (mb)\n");
            writer.close();
            writer = new BufferedWriter(new FileWriter("results.out"));
            writer.close();
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }
    }

    public static void toFile(CSVLine line) {

        try {
            // Write stats
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv", true));
            String str = line.query + ", " + line.numOfKeyword + ", " + line.totalMappings + ", " + 
                line.numOfTupleSets + "," + line.numOfRelations + ", " + line.cnetworks + ", " + line.sqlQueries + ", " +
                line.numOfTuples + ", ";            
            line.totalTime = 0D;
            String timestr = new String();
            for (Double time: line.timerPerFunction) {
                line.totalTime += time;
                timestr += time + ", ";
            }
            str += line.totalTime + ", " + timestr.substring(0, timestr.length() - 2) + ", ";

            // Write IO-CPU-Mem
            str += line.sqlIO + ", "
                 + line.cpuIndex + ", " + line.cpuCNetGen +  "," 
                 + line.memIndex  + ", " + line.memCNetGen + "\n";

            writer.write(str);
            writer.close();

            // Write the results 
            // BufferedWriter resWriter = new BufferedWriter(new FileWriter("results.out", true));            
            // str = new String();
            // str += line.query + "\n";
            // for (OverloadedTupleList tup : line.tuples) {
            //     str += "Candiate Network generated results below: " + tup.getCnGeneratedThis() + "\n";
            //     str += tup.toString() + "\n";
            // }
            // str += "\n\n==================================================\n\n";
            // resWriter.write(str);
            // resWriter.close();
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }

    }

}