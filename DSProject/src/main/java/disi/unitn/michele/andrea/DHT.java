package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DHT {

    public Map<Integer, ActorRef> HashTable;
    public Set<ActorRef> AvailableNodes;

    public DHT() {
        HashTable = new HashMap<>();
        AvailableNodes = new HashSet<>();
    }

    public void PrintNetwork() {
        HashTable.forEach((key, value) -> System.out.println("\t" + key + " " + value));
        System.out.println();
    }
}
