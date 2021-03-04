package expressq2.testing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;


public class InfoWriter {

    public static void init() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv"));
            // Write titles 
            writer.write(
                "query, #keywords, #totalMappings, #MappingsPerKW (Metadata & Rows), " + 
                " #ofAnnotatedQueries, #ofQueryPatterns, Results, TotalTime,"+ 
                " TimeQueryAnalyzer, TimeQueryInterpreter, TimeQueryTranslator, TimeQueryExecutor, " +
                " Sql-IO (b), CPU Q Ana, CPU Q Inter, CPU Q Trans, Mem Ana (mb), Mem Q Inter (mb), Mem Q Trans\n"
            );
            writer.close();           
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }
    }

    /**
     * Write the systems answer for one query in the stats file.
     * 
     * @param answer
     */
    public static void toFile(Info answer) {

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("stats.csv", true));

            // Write answer
            String str = answer.query + ", ";     // The answer to write in the output file

            // Write mappings per kw            
            String kwMappings = "{";
            answer.totalMappings = 0;            
            for (Map.Entry<String, SimpleEntry<Integer, Integer>> entry: answer.mappingsPerKW.entrySet()) {
                kwMappings += entry.getKey() + " : " + entry.getValue().getKey() + " + " + entry.getValue().getValue()  + " | ";
                answer.totalMappings += entry.getValue().getKey();
            }
            answer.totalMappings /= answer.mappingsPerKW.size();

            if (answer.mappingsPerKW.size() > 0)
                str += answer.keywords + ", " + answer.totalMappings + ", " +  kwMappings.substring(0, kwMappings.length() - 2) + "} , " + 
                    answer.numOfAnnotatedQueries + ", " + answer.numOfQueryPatters + ", " + answer.numOfResults + ", " ;
            else 
                str += answer.keywords + ", " + answer.totalMappings + ", " +  "{} , " + 
                    answer.numOfAnnotatedQueries + ", " + answer.numOfQueryPatters + ", " + answer.numOfResults + ", " ;
            
            // Write times
            answer.totalTime = 0D;
            String timeStr = new String();
            for (Double entry: answer.timerPerComponent) {
                answer.totalTime += entry;
                timeStr += entry + ", ";
            }
            str += answer.totalTime + ", " + timeStr.substring(0, timeStr.length() - 2) + ",";


            // Write IO-CPU-Mem
            str += answer.sqlIO + ", "
                 + answer.cpuQueryAn + ", " + answer.cpuQueryInter + ", " + answer.cpuQueryTrans +  "," 
                 + answer.memQueryAn + ", " + answer.memQueryInter + ", " + answer.memQueryTrans + "\n";

            // Write the str in the file
            writer.write(str);
            writer.close();           
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }

    }


}