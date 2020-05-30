// javac -cp ./couchbase/*:. Convert.java
// java -cp ./couchbase/*:. Convert

import java.io.File;
import java.io.IOException;

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

public class Convert {
    public static void print() {
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            final DocumentBuilder builder = factory.newDocumentBuilder();
            final Document document = builder.parse(new File("Invoice.xml"));

            final Element racine = document.getDocumentElement();
            Bucket bucket = connect("invoices");
            Collection collection = bucket.defaultCollection();

            final NodeList racineNoeuds = racine.getChildNodes();
            final int nbRacineNoeuds = racineNoeuds.getLength();
            for (int i = 0; i < nbRacineNoeuds; i++) {
                if(racineNoeuds.item(i).getNodeType() == Node.ELEMENT_NODE) {
                    final Element invoice = (Element)racineNoeuds.item(i);

                    String OrderId = invoice.getElementsByTagName("OrderId").item(0).getTextContent();
                    long PersonId = Long.parseLong(invoice.getElementsByTagName("PersonId").item(0).getTextContent());
                    String OrderDate = invoice.getElementsByTagName("OrderDate").item(0).getTextContent();
                    float TotalPrice = Float.parseFloat(invoice.getElementsByTagName("TotalPrice").item(0).getTextContent());

                    JsonArray OrderLines = JsonArray.create();

                    final NodeList orderLines = invoice.getChildNodes();
                    final int nbOrderLines = orderLines.getLength();
                    for (int j = 0; j < nbOrderLines; j++) {
                        if(orderLines.item(j).getNodeType() == Node.ELEMENT_NODE
                            && orderLines.item(j).getNodeName().equals("Orderline")
                        ) {
                            final Element orderLine = (Element)orderLines.item(j);

                            int productId = Integer.parseInt(orderLine.getElementsByTagName("productId").item(0).getTextContent());
                            String asin = orderLine.getElementsByTagName("asin").item(0).getTextContent();
                            String title = orderLine.getElementsByTagName("title").item(0).getTextContent();
                            float price = Float.parseFloat(orderLine.getElementsByTagName("price").item(0).getTextContent());
                            String brand = orderLine.getElementsByTagName("brand").item(0).getTextContent();

                            JsonObject OrderLine = JsonObject.create()
                                .put("productId", productId)
                                .put("asin", asin)
                                .put("title", title)
                                .put("price", price)
                                .put("brand", brand);
                            OrderLines.add(OrderLine);
                        }
                    }

                    JsonObject invoiceObject = JsonObject.create()
                        .put("PersonId", PersonId)
                        .put("OrderDate", OrderDate)
                        .put("TotalPrice", TotalPrice)
                        .put("OrderLines", OrderLines);
                    insert(collection, invoiceObject, OrderId);
                }
            }
        } catch (final ParserConfigurationException e) {
            e.printStackTrace();
        } catch (final SAXException e) {
            e.printStackTrace();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public static Bucket connect(String bucketName) {
        Cluster cluster = Cluster.connect("couchbase://149.91.80.197", "Administrator", "");
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
        Convert.print();
    }
}
