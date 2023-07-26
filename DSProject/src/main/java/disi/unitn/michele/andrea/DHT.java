package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.Map;

public class DHT {

    public static Map<Integer, ActorRef> HashTable = null;

    public DHT() {
        HashTable = new HashMap<>();
    }

    public void PrintNetwork() {
        HashTable.forEach((key, value) -> System.out.println(key + " " + value));
        System.out.println();
    }
}
