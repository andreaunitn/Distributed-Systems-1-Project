package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DHT {

    public Map<Integer, ActorRef> hash_table;
    public Set<ActorRef> available_nodes;

    /***** Constructor *****/
    public DHT() {
        this.hash_table = new HashMap<>();
        this.available_nodes = new HashSet<>();
    }

    // Print all nodes of the network (also the crashed ones) with the corresponding content
    public void PrintNetwork() {

        this.hash_table.forEach((key, node) -> {

            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            node.tell(new MessageNode.PrintNodeMsg(), ActorRef.noSender());
        });
    }
}
