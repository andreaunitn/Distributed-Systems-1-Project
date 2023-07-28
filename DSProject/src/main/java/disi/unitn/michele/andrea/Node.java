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

        this.rnd = new Random();
        this.storage = new HashMap<>();


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
                .match(Message.NodeAnnounceMsg.class, this::OnNodeAnnounceMsg)
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

            if(((k <= this.key && k >= 0) || (k > m.key && k <= 1023)) && k != m.key) {
                this.storage.put(k, v);
            }
        }

        // Multicast to every other nodes in the network
        Multicast(new Message.NodeAnnounceMsg(this.key), new HashSet<ActorRef>(this.network.values()));
    }

    // Node that receives a multicast message
    private void OnNodeAnnounceMsg(Message.NodeAnnounceMsg m) {
        this.network.put(m.key, getSender());
        HashSet<Integer> keySet = new HashSet(this.storage.keySet());

        for(Integer k : keySet) {
            System.out.println("Key: " + k);
            if((k <= m.key && k >= 0) || (k > this.key && k <= 1023)) {
                System.out.println("Value: " + storage.get(k));
                this.storage.remove(k);
            }
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
        Integer neighborKey;

        Set<Integer> keySet = this.network.keySet();
        ArrayList<Integer> keyList = new ArrayList<>(keySet);
        Collections.sort(keyList);

        for(int i = 0; i < keyList.size(); i++) {
            if(keyList.get(i) > this.key) {
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

    private boolean IsInInterval(Integer myKey, Integer nextKey, Integer value) {
        boolean result = false;

        if(myKey < nextKey) {
            if()
        } else {

        }

        return result;
    }

    // TODO
    public void Leave() {}
    public void Recovery() {}
    public void Update(Integer key, int value) {}
    public void Get() {}
    public void ForwardRequest() {}
}
