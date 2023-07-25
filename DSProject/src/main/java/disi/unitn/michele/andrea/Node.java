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

    // TODO 4: To be adapter for our code
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartChatMsg.class,    this::onStartChatMsg)
                .match(ChatMsg.class,         this::onChatMsg)
                .match(PrintHistoryMsg.class, this::printHistory)
                .build();
    }

    public void Update(Integer key, int value) {}
    public int Get(Integer key) {}
    public void ForwardRequest() {}

    public void Join() {}
    public void Leave() {}
    public void Recovery() {}
}
