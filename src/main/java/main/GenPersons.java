// javac -cp ./couchbase/*:. GenPersons.java
// java -cp ./couchbase/*:. GenPersons

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

public class GenPersons {
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
        Bucket bucket = connect("customers");
        Collection collection = bucket.defaultCollection();

        ArrayList<String> postsIds = new ArrayList<String>();
        ArrayList<String> personsIds = new ArrayList<String>();

        ArrayList<String> personsIds2 = new ArrayList<String>();
        ArrayList<String> tagsIds = new ArrayList<String>();

        ArrayList<String> personsIds3 = new ArrayList<String>();
        ArrayList<String> personsIds4 = new ArrayList<String>();
        ArrayList<String> dates = new ArrayList<String>();

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
            br = new BufferedReader(new FileReader("./person_hasInterest_tag_0_0.csv"));

            br.readLine();

            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|");

                personsIds2.add(parts[0]);
                tagsIds.add(parts[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            br = new BufferedReader(new FileReader("./person_knows_person_0_0.csv"));

            br.readLine();

            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|");

                personsIds3.add(parts[0]);
                personsIds4.add(parts[1]);
                dates.add(parts[2]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File postsFile = new File("Persons.json");
        PrintWriter pw = null;

        try {
            br = new BufferedReader(new FileReader("./person_0_0.csv"));

            pw = new PrintWriter(postsFile);
            br.readLine();

            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|");

                JsonArray posts = JsonArray.create();
                for (int i = postsIds.size() - 1; i >= 0; i--) {
                    if (personsIds.get(i).equals(parts[0])) {
                        posts.add(postsIds.get(i));
                        postsIds.remove(i);
                        personsIds.remove(i);
                    }
                }

                JsonArray interests = JsonArray.create();
                for (int i = personsIds2.size() - 1; i >= 0; i--) {
                    if (personsIds2.get(i).equals(parts[0])) {
                        interests.add(tagsIds.get(i));
                        personsIds2.remove(i);
                        tagsIds.remove(i);
                    }
                }

                JsonArray knows = JsonArray.create();
                for (int i = personsIds3.size() - 1; i >= 0; i--) {
                    if (personsIds3.get(i).equals(parts[0])) {
                        knows.add(JsonObject.create()
                            .put("id", personsIds4.get(i))
                            .put("creationDate", dates.get(i))
                        );
                    } else if (personsIds4.get(i).equals(parts[0])) {
                        knows.add(JsonObject.create()
                            .put("id", personsIds3.get(i))
                            .put("creationDate", dates.get(i))
                        );
                    }
                }

                JsonObject invoiceObject = JsonObject.create()
                    .put("id", parts[0])
                    .put("firstName", parts[1])
                    .put("lastName", parts[2])
                    .put("gender", parts[3])
                    .put("birthday", parts[4])
                    .put("creationDate", parts[5])
                    .put("locationIP", parts[6])
                    .put("browserUsed", parts[7])
                    .put("place", Integer.parseInt(parts[8]))
                    .put("posts", posts)
                    .put("interests", interests)
                    .put("knows", knows);
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
