package disi.unitn.michele.andrea;

import akka.event.LoggingAdapter;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

public class Node extends AbstractActor {

    private final Random rnd;
    private final Integer key;
    private boolean IsCoordinator = false;
    private boolean IsCrashed = false;

    private HashMap<Integer, ActorRef> network;
    private HashMap<Integer, String> storage;
    private List<ActorRef> fingerTable;

    // Logger
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public Node(Integer key) {
        this.key = key;

        rnd = new Random();
        storage = new HashMap<>();
        fingerTable = new ArrayList<>();

        network = new HashMap<>();
        network.put(key, getSelf());

        for(int i = 0; i < 10; i++) {
            fingerTable.add(ActorRef.noSender());
        }
    }

    //
    static public Props props(Integer key) {
        return Props.create(Node.class, () -> new Node(key));
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(Message.JoinNetworkOrder.class, this::OnJoinOrder)
                .match(Message.JoinRequestMsg.class, this::OnJoinRequest)
                .match(Message.JoinResponseMsg.class, this::OnBootstrapResponse)
                .match(Message.DataRequestMsg.class, this::OnDataRequest)
                .match(Message.DataResponseMsg.class, this::OnDataResponse)
                .build();
    }

    // New node function
    private void OnJoinOrder(Message.JoinNetworkOrder m) {
        if (m != null) {
            Message.JoinRequestMsg msg = new Message.JoinRequestMsg(key, getSelf());
            m.bootstrapNode.tell(msg, getSelf());
        }
    }

    // Bootstrapper function
    private void OnJoinRequest(Message.JoinRequestMsg m) {
        Message.JoinResponseMsg msg = new Message.JoinResponseMsg(network, getSelf());
        m.sender.tell(msg, getSelf());
    }

    // New node function
    private void OnBootstrapResponse(Message.JoinResponseMsg m) {
        // Update knowledge of the network
        HashMap<Integer, ActorRef> received_network = m.network;
        network.putAll(received_network);

        // Find neighbor
        Integer neighborKey = FindNeighbor();
        ActorRef node = network.get(neighborKey);

        // Contact neighbor and request data
        node.tell(new Message.DataRequestMsg(getSelf()), getSelf());

        // Filter data based on the key

        // Read operations

        // Multicast

        // Remove unnecessary data from other nodes
    }

    // Send storage to the new node
    private void OnDataRequest(Message.DataRequestMsg m) {
        m.sender.tell(new Message.DataResponseMsg(storage), getSelf());
    }

    // New node receive the storage
    private void OnDataResponse(Message.DataResponseMsg m) {
        // Take only necessary data
    }

    //
    public void Leave() {}
    public void Recovery() {}
    public void Update(Integer key, int value) {}
    public void ForwardRequest() {}

    private Integer FindNeighbor() {
        Integer neighborKey;

        Set<Integer> keySet = network.keySet();
        ArrayList<Integer> keyList = new ArrayList<>(keySet);
        Collections.sort(keyList);

        for(int i = 0; i < keyList.size(); i++) {
            if(keyList.get(i) > key) {
                neighborKey = keyList.get(i);
                return neighborKey;
            }
        }

        // No bigger key found, take the first
        neighborKey = keyList.get(0);
        return neighborKey;
    }

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
