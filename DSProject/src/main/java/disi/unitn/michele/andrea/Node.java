package disi.unitn.michele.andrea;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Node extends AbstractActor {

    public final Integer key;
    public boolean IsCoordinator = false;
    public boolean IsCrashed = false;

    //TODO 1: add attribute for having information about the system
    //TODO 2: add structure to contain node data
    //TODO 3: Structure to manage data

    public Node(Integer key) {
        this.key = key;
    }

    static public Props props(Integer key) {
        return Props.create(Node.class, () -> new Node(key));
    }

    // TODO 4: To be adapted for our code
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .build();
    }

    public void Update(Integer key, int value) {}
    //public int Get(Integer key) {}
    public void ForwardRequest() {}

    public void Join() {}
    public void Leave() {}
    public void Recovery() {}
}
