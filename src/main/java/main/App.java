package main;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;

public class App {

  public static void main(String... args) {
    Cluster cluster = Cluster.connect("couchbase://filipedoutelsilva.com", "Administrator", "bFtn7&@Do8!Xqu");
    Bucket bucket = cluster.bucket("testbucket");
    Collection collection = bucket.defaultCollection();

    try {
      JsonObject vendor1 = JsonObject.create().put("Country", "France").put("Industry", "Travel").put("Vendor", "TWA");
      String idVendor1 = "TWA3";
      collection.insert(idVendor1, vendor1);

      JsonObject vendorToDelete = JsonObject.create()
          .put("Country", "Wonderland")
          .put("Industry", "bee")
          .put("Vendor", "wdl");
      String idVendorToDelete = "WDL3";
      collection.insert(idVendorToDelete, vendorToDelete);
      collection.remove(idVendorToDelete);

      JsonObject vendorToUpdate = JsonObject.create()
          .put("Country", "China")
          .put("Industry", "voiturier")
          .put("Vendor", "cnxx");
      String idVendorToUpdate = "CNXX3";
      collection.insert(idVendorToUpdate, vendorToUpdate);
      JsonObject VendorUpdated = JsonObject.create().put("Country", "Russia");
      collection.upsert(idVendorToUpdate, VendorUpdated);
    } catch (DocumentNotFoundException ex) {
      System.out.println(ex);
    }
  }
}
