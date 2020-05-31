// javac -cp ./couchbase/*:. GenPosts.java
// java -cp ./couchbase/*:. GenPosts

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;

import com.couchbase.client.java.kv.MutationResult;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonArray;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentExistsException;

public class GenPosts {
    public static Bucket connect(String bucketName) {
        Cluster cluster = Cluster.connect("couchbase://filipedoutelsilva.com", "Administrator", "");
        Bucket bucket = cluster.bucket(bucketName);
        return bucket;
    }

    public static void insert(Collection collection, JsonObject content, String id) {
        try {
          MutationResult insertResult = collection.insert(id, content);
        } catch (DocumentExistsException ex) {
          System.err.println("The document already exists!");
        } catch (CouchbaseException ex) {
          System.err.println("Something else happened: " + ex);
        }
    }

    public static void main(String[] args) {
        Bucket bucket = connect("posts");
        Collection collection = bucket.defaultCollection();

        ArrayList<String> postsIds = new ArrayList<String>();
        ArrayList<String> personsIds = new ArrayList<String>();

        ArrayList<String> postsIds2 = new ArrayList<String>();
        ArrayList<String> tagsIds = new ArrayList<String>();

        BufferedReader br = null;
        String line = "";

        try {
            br = new BufferedReader(new FileReader("./post_hasCreator_person_0_0.csv"));

            br.readLine();

            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|");

                postsIds.add(parts[0]);
                personsIds.add(parts[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            br = new BufferedReader(new FileReader("./post_hasTag_tag_0_0.csv"));

            br.readLine();

            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|");

                postsIds2.add(parts[0]);
                tagsIds.add(parts[0]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File postsFile = new File("Posts.json");
        PrintWriter pw = null;

        try {
            br = new BufferedReader(new FileReader("./post_0_0.csv"));

            pw = new PrintWriter(postsFile);
            br.readLine();

            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|");

                String creator = "";
                for (int i = 0; i < postsIds.size(); i++) {
                    if (postsIds.get(i).equals(parts[0])) {
                        creator = personsIds.get(i);
                    }
                }

                JsonArray tags = JsonArray.create();
                for (int i = 0; i < postsIds2.size(); i++) {
                    if (postsIds2.get(i).equals(parts[0])) {
                        tags.add(tagsIds.get(i));
                    }
                }

                JsonObject invoiceObject = JsonObject.create()
                    .put("createDate", parts[2])
                    .put("location", parts[3])
                    .put("browserUsed", parts[4])
                    .put("language", parts[5])
                    .put("content", parts[6])
                    .put("length", Integer.parseInt(parts[7]))
                    .put("creator", creator)
                    .put("tags", tags);
                pw.println(invoiceObject.toString());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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
