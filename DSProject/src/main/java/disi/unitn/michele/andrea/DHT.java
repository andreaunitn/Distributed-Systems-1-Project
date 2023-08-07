package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.util.concurrent.TimeUnit;
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

    // Print all nodes of the network (also the crashed ones) with the corresponding content
    public void PrintNetwork() {

        HashTable.forEach((key, value) -> {

            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            value.tell(new Message.PrintNode(), ActorRef.noSender());
        });
    }
}
