// javac -cp ./couchbase/*:. GenTags.java
// java -cp ./couchbase/*:. GenTags

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Iterator;

public class GenTags {
    public static void main(String[] args) {
        HashSet<String> tags = new HashSet<String>();
        HashSet<String> words = new HashSet<String>();

        String csvFile = "./post_hasTag_tag_0_0.csv";
        BufferedReader br = null;
        String line = "";
        String separator = "\\|";

        try {
            br = new BufferedReader(new FileReader("./dictionary.txt"));

            while ((line = br.readLine()) != null) {
                words.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            br = new BufferedReader(new FileReader(csvFile));

            br.readLine(); // Remove the first line

            while ((line = br.readLine()) != null) {
                String[] parts = line.split(separator);

                if (!tags.contains(parts[1])) {
                    tags.add(parts[1]);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File tagsFile = new File("Tags.csv");
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(tagsFile);

            Iterator<String> it = tags.iterator();
            Iterator<String> wordsIt = words.iterator();
            while (it.hasNext()) {
                String tag = it.next();
                pw.println(tag + "|" + wordsIt.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (pw != null) {
                pw.flush();
                pw.close();
            }
        }
    }
}