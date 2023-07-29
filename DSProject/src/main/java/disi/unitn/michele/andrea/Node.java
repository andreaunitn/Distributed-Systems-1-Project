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
    private boolean isCoordinator = false;
    private boolean isCrashed = false;
    private int valuesToCheck = 0;

    private HashMap<Integer, ActorRef> network;
    private HashMap<Integer, String> storage;
    private List<ActorRef> fingerTable;

    // Logger
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public Node(Integer key) {
        this.key = key;

        this.rnd = new Random();
        this.storage = new HashMap<>();

        // TODO: remove when finished
        if(this.key == 2) {
            for (int i = 0; i < 5; i++) {
                Integer number = rnd.nextInt(20);
                this.storage.put(number, "ciao");
            }
        }

        this.fingerTable = new ArrayList<>();

        this.network = new HashMap<>();
        this.network.put(key, getSelf());

        for(int i = 0; i < 10; i++) {
            this.fingerTable.add(ActorRef.noSender());
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
                .match(Message.PrintNode.class, this::OnPrintNode)
                .match(Message.NodeAnnounceMsg.class, this::OnNodeAnnounce)
                .match(Message.ReadRequestMsg.class, this::OnReadRequest)
                .match(Message.ReadResponseMsg.class, this::OnReadResponse)
                .build();
    }

    // New node contacts bootstrapper
    private void OnJoinOrder(Message.JoinNetworkOrder m) {
        if (m != null) {
            Message.JoinRequestMsg msg = new Message.JoinRequestMsg(this.key, getSelf());
            m.bootstrapNode.tell(msg, getSelf());
        }
    }

    // Bootstrapper sends to new node the current topology of the network
    private void OnJoinRequest(Message.JoinRequestMsg m) {
        Message.JoinResponseMsg msg = new Message.JoinResponseMsg(this.network, getSelf());
        m.sender.tell(msg, getSelf());
    }

    // New node perform actions after receiving network topology from the bootstrapper
    private void OnBootstrapResponse(Message.JoinResponseMsg m) {
        // Update knowledge of the network
        Map<Integer, ActorRef> received_network = m.network;
        this.network.putAll(received_network);

        // Find neighbor
        Integer neighborKey = FindNeighbor();
        ActorRef node = this.network.get(neighborKey);

        // Contact neighbor and request data
        node.tell(new Message.DataRequestMsg(getSelf()), getSelf());

        // Filter data based on the key
        // Read operations
        // Multicast
        // Remove unnecessary data from other nodes
    }

    // Send storage to the new node
    private void OnDataRequest(Message.DataRequestMsg m) {
        m.sender.tell(new Message.DataResponseMsg(this.storage, this.key), getSelf());
    }

    // Node receives the data storage from the neighbor
    private void OnDataResponse(Message.DataResponseMsg m) {
        // Take only necessary data
        Map<Integer, String> s = m.storage;

        for(Map.Entry<Integer, String> entry: s.entrySet()) {
            Integer k = entry.getKey();
            String v = entry.getValue();

            if(IsInInterval(this.key, m.key, k)) {
                this.valuesToCheck++;
                this.storage.put(k, v);
                getSender().tell(new Message.ReadRequestMsg(getSelf(), k), getSelf());
            }
        }
    }

    // Node that receives a multicast message
    private void OnNodeAnnounce(Message.NodeAnnounceMsg m) {
        this.network.put(m.key, getSender());
        HashSet<Integer> keySet = new HashSet(this.storage.keySet());

        for(Integer k : keySet) {
            if(IsInInterval(m.key, this.key, k)) {
                this.storage.remove(k);
            }
        }
    }

    // Accepts read request
    private void OnReadRequest(Message.ReadRequestMsg m) {
        if(this.storage.containsKey(m.key)) {
            getSender().tell(new Message.ReadResponseMsg(m.sender, m.key, storage.get(m.key)), getSelf());
        } else {
            ActorRef holdingNode = this.network.get(FindNeighbor(m.key));
            holdingNode.tell(new Message.ReadRequestMsg(m.sender, m.key), getSelf());
        }
    }

    private void OnReadResponse(Message.ReadResponseMsg m) {
        // Check data is OK
        this.storage.put(m.key, m.value);
        this.valuesToCheck--;

        if(this.valuesToCheck == 0) {
            // Multicast to every other nodes in the network
            Multicast(new Message.NodeAnnounceMsg(this.key), new HashSet<ActorRef>(this.network.values()));
        }
    }

    // Print node storage
    private void OnPrintNode(Message.PrintNode m) {
        System.out.println("\t Node: " + this.key);
        for(Map.Entry<Integer, String> entry: this.storage.entrySet()) {
            System.out.println("\t\t" + " Key: " + entry.getKey() + " Value: " + entry.getValue());
        }

        System.out.println();
    }

    //
    // Find the node with the next key
    private Integer FindNeighbor() {
        return FindNeighbor(this.key);
    }

    // Find the node with key k
    private Integer FindNeighbor(Integer k) {
        Integer neighborKey;

        Set<Integer> keySet = this.network.keySet();
        ArrayList<Integer> keyList = new ArrayList<>(keySet);
        Collections.sort(keyList);

        for(int i = 0; i < keyList.size(); i++) {
            if(keyList.get(i) > k) {
                neighborKey = keyList.get(i);
                return neighborKey;
            }
        }

        // No bigger key found, take the first (circular ring)
        neighborKey = keyList.get(0);
        return neighborKey;
    }

    // Perform multicast to every other node in the network
    private int Multicast(Serializable m, Set<ActorRef> multicastGroup) {
        int i = 0;
        for (ActorRef r: multicastGroup) {

            // send m to r (except to self)
            if (!r.equals(getSelf())) {

                // model a random network/processing delay
                try { Thread.sleep(this.rnd.nextInt(10)); }
                catch (InterruptedException e) { e.printStackTrace(); }

                r.tell(m, getSelf());
                i++;
            }
        }

        return i;
    }

    // Returns true if value should be saved in the new node
    private boolean IsInInterval(Integer newNodeKey, Integer nextNodeKey, Integer value) {
        if(newNodeKey > nextNodeKey) {
            if(value <= newNodeKey && value > nextNodeKey) {
                return true;
            }
        } else {
            if(value > nextNodeKey || value <= newNodeKey) {
                return true;
            }
        }

        return false;
    }

    // TODO
    public void Leave() {}
    public void Recovery() {}
    public void Update(Integer key, int value) {}
    public void Get() {}
    public void ForwardRequest() {}
}
