package spark.testing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import spark.model.OverloadedTuple;

public class CSVManager {

    public static void init() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv"));
            BufferedWriter top10resWriter = new BufferedWriter(new FileWriter("top10.out"));

            // Write titles
            writer.write("query, #ofKWs, #Mappings, #ofInitRelations, #ofTupleSets," +
                        " #ofCandidateNetworks, #ofSQLQueries, #ofResults, TotalTime, " +
                        "Time IREngine, Time CandidateNetworkGenerator, Time Executor," +
                        " Sql-IO (b), CPU Index, CPU CNet Gen, Mem Index (mb), Mem CNet Gen (mb)\n");
            writer.close();
            top10resWriter.close();
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }
    }

    public static void toFile(CSVLine line) {

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv", true));
            // BufferedWriter top10resWriter = new BufferedWriter(new FileWriter("top10.out", true));

            // Write the stats {
            String str = line.query + ", " + line.numOfKeyword + ", " + line.totalMappings + ", " +
                line.numOfRelations + ", " + line.numOfTupleSets + ", " + line.numOfCandidateNetworks + ", " + line.sqlQueries + ", " +
                line.numOfTuples + ", ";

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
                 + line.cpuIndex + ", " + line.cpuCNetGen +  ","
                 + line.memIndex  + ", " + line.memCNetGen + "\n";

            // Write the str in the new file
            writer.write(str);
            writer.close();

            // // Write the Top 10 res
            // str = "Query: "  + line.query;
            // for (OverloadedTuple tup: line.topRes) {
            //     str += tup.toString() + "\n";
            // }
            // str += "\n=================================\n\n";
            // top10resWriter.write(str);


            // top10resWriter.close();
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }

    }


}