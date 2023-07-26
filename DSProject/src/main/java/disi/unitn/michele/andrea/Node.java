package disi.unitn.michele.andrea;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

import disi.unitn.michele.andrea.Message;

public class Node extends AbstractActor {

    private final Random rnd;
    private final Integer key;
    private boolean IsCoordinator = false;
    private boolean IsCrashed = false;
    private HashMap<Integer, String> storage;
    private Set<ActorRef> network;
    private List<ActorRef> fingerTable;

    //TODO 1: add attribute for having information about the system
    //TODO 2: add structure to contain node data
    //TODO 3: functions to manage storage

    public Node(Integer key) {
        this.rnd = new Random();
        this.key = key;
        this.storage = new HashMap<>();
        this.fingerTable = new ArrayList<>();
        for(int i = 0; i<10; i++) {
            this.fingerTable.add(ActorRef.noSender());
        }
    }

    static public Props props(Integer key) {
        return Props.create(Node.class, () -> new Node(key));
    }

    // TODO 4: To be adapted for our code
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(Message.JoinNetworkOrder.class, this::OnJoinOrder)
                .match(Message.JoinRequestMsg.class, this::OnJoinRequest)
                .build();
    }

    public void Update(Integer key, int value) {}
    //public int Get(Integer key) {}
    public void ForwardRequest() {}

    private void OnJoinOrder(Message.JoinNetworkOrder m) {
        if (m != null) {
            Message.JoinRequestMsg msg = new Message.JoinRequestMsg(key, getSelf());
            m.bootstrapNode.tell(msg, getSelf());
        }
    }

    private void OnJoinRequest(Message.JoinRequestMsg m) {
        network.add(m.sender);

        //TODO
    }

    public void Leave() {}
    public void Recovery() {}

    private int Multicast(Serializable m, Set<ActorRef> multicastGroup) {
        int i = 0;
        for (ActorRef r: multicastGroup) {

            // check if the node should crash
            /*if(m.getClass().getSimpleName().equals(nextCrash.name())) {
                if (i >= nextCrashAfter) {
                    //System.out.println(getSelf().path().name() + " CRASH after " + i + " " + nextCrash.name());
                    break;
                }
            }*/

            // send m to r (except to self)
            if (!r.equals(getSelf())) {

                // model a random network/processing delay
                try { Thread.sleep(rnd.nextInt(10)); }
                catch (InterruptedException e) { e.printStackTrace(); }

                r.tell(m, getSelf());
                i++;
            }
        }

        return i;
    }

}
