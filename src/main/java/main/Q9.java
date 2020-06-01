package main;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryResult;

public class Q9 {
    static Cluster cluster;

    public static QueryResult question9(String country, int year) {
        String query = "SELECT orderList.brand, COUNT(orderList.brand) AS nbMarque FROM orders UNNEST Orderline orderList LET brand = ( SELECT Vendor AS brand FROM vendors WHERE Country = '" + country + "' ) WHERE orders.OrderDate > '" + year + "-01-01' AND orders.OrderDate < '" + year + "-31-12' GROUP BY orderList.brand ORDER BY nbMarque DESC LIMIT 3";
        return cluster.query(query);
    }

    public static void main(String... args) {
        Cluster.connect("couchbase://filipedoutelsilva.com", "Administrator", "bFtn7&@Do8!Xqu");
        System.out.println(question9("China", 2020).rowsAsObject());
    }
}
