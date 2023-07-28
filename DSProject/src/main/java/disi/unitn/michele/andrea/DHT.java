package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DHT {

    public Map<Integer, ActorRef> HashTable;
    public Set<ActorRef> AvailableNodes;

    public DHT() {
        HashTable = new HashMap<>();
        AvailableNodes = new HashSet<>();
    }

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
