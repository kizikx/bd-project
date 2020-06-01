package main;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryResult;

public class Q2 {
    static Cluster cluster;

    public static QueryResult question2(String asin, int year) {
        String query = "SELECT Bought.person FROM (SELECT orders.PersonId AS person, orderList.asin FROM orders UNNEST Orderline orderList WHERE orders.OrderDate > '" + year + "-01-01' AND orders.OrderDate < '" + year + "-31-12') Bought JOIN feedbacks ON Bought.PersonId = feedbacks.PersonId WHERE feedbacks.asin = '" + asin + "'";
        return cluster.query(query);
    }

    public static void main(String... args) {
        Cluster.connect("couchbase://filipedoutelsilva.com", "Administrator", "bFtn7&@Do8!Xqu");
        System.out.println(question2("China", 2020).rowsAsObject());
    }
}
