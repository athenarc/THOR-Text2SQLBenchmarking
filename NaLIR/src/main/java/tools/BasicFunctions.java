package tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.lang3.math.NumberUtils;

public class BasicFunctions
{
    public static void main(String [] args) throws IOException
    {
        String fileName = "src/zfiles/allQueryResults.txt";
        writeFile(fileName, "tong", true);
        // readFile("zfiles/tokens.xml");
    }

    @SuppressWarnings("resource")
    public static String readFile(String fileName) throws IOException  //read file to a String
    {
        String result = "";

        // System.out.println("Trying to read '" + fileName + "'");
        InputStream is = ClassLoader.getSystemResourceAsStream(fileName);
        if (is != null) {
            InputStreamReader isr = new InputStreamReader(is, "GBk");
            BufferedReader bfin = new BufferedReader(isr);
            String rLine = "";
            while((rLine = bfin.readLine()) != null)
            {
                result += rLine + "\n";
            }
        }
        else {
            System.out.println("no such file");
        }
		
        // ClassLoader classLoader = BasicFunctions.class.getClassLoader();
        // File file = new File(classLoader.getResource(fileName).getFile());
        // if(file.exists())
        // {
        //     FileInputStream fi = new FileInputStream(file);
        //     InputStreamReader isr = new InputStreamReader(fi, "GBk");
        //     BufferedReader bfin = new BufferedReader(isr);
        //     String rLine = "";
        //     while((rLine = bfin.readLine()) != null)
        //     {
        //         result += rLine + "\n";
        //     }
        // }
        // else
        // {
        //     System.out.println("no such file");
        // }

        return result;
    }

    public static void writeFile(String fileName, String result, boolean append) throws IOException
    {
        FileWriter writer = new FileWriter(fileName, append);
        writer.write(result);
        writer.close();
    }

    public static Object depthClone(Object srcObj)
    {
        Object cloneObj = null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(out);
            oo.writeObject(srcObj);

            ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
            ObjectInputStream oi = new ObjectInputStream(in);
            cloneObj = oi.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return cloneObj;
    }

    public static boolean isNumeric(String str)
    {
        return NumberUtils.isCreatable(str);
    }
}
