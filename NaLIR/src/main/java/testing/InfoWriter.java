package testing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;


public class InfoWriter {

    public static void init() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv"));

            // Write titles 
            writer.write(
                "query, #totalMappings, #MappingsPerKW (Metadata & Rows), #ofIntermediateRepresentations, Results, TotalTime,"+ 
                " TimeDependencyParser, TimeNodeMapper, TimeTreeAdjustor, TimeTranslator&executor, " +
                " Sql-IO (b), CPU Parser, CPU Mapper, CPU EntRes, CPU Translator, Mem Parser (mb), Mem Mapper (mb), Mem EntRes, Mem Translator\n"                
            );            
            writer.close();           
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }
    }

    public static void toFile(Info answer) {

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv", true));
            

            // Write answers            
            String str = answer.query + ", ";     // The answer to write in the output file

            // Write mappings per kw
            String mapps = "{";
            answer.totalMappings = 0;
            for (Map.Entry<String, SimpleEntry<Integer, Integer>> entry: answer.mappingsPerKW.entrySet()) {
                mapps += entry.getKey() + " : " + entry.getValue().getKey() + " + " + entry.getValue().getValue()  + " | ";
                answer.totalMappings += entry.getValue().getKey();
            }
            if (answer.mappingsPerKW.size() != 0)
                answer.totalMappings /= answer.mappingsPerKW.size();

            str += answer.totalMappings + ", " + mapps.substring(0, mapps.length() - 2) + "} , " + answer.numOfIRepresentations + ", " + answer.numOfResults + ", " ;
            
            // Write times
            answer.totalTime = 0D;
            String timeStr = new String();
            for (Double entry: answer.timerPerComponent) {
                answer.totalTime += entry;
                timeStr += entry + ", ";
            }
            str += answer.totalTime + ", " + timeStr.substring(0, timeStr.length() - 2) + ", ";

            // Write IO-CPU-Mem
            str += answer.sqlIO + ", "
                + answer.cpuParser + ", " + answer.cpuMapper + ", " + answer.cpuEntRes + ", " + answer.cpuTranslator +  "," 
                + answer.memParser + ", " + answer.memMapper + ", " + answer.memEntRes + ", " + answer.memTranslator + "\n";
       

            // Write the str in the new file
            writer.write(str);            
            writer.close();           
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }

    }


}