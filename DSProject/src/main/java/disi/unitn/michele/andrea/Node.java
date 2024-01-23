package disi.unitn.michele.andrea;

import scala.concurrent.duration.Duration;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {

    private final Random rnd;
    private final Integer key;
    private boolean isCrashed = false;
    private boolean isJoining = false;
    private boolean isRecovering = false;
    private int valuesToCheck = 0;

    private HashMap<Integer, ActorRef> network;
    private HashMap<Integer, DataEntry> storage;
    private HashSet<Message.WriteRequestMsg> writeRequests;

    public Node(Integer key) {
        this.key = key;
        this.rnd = new Random();
        this.storage = new HashMap<>();
        this.network = new HashMap<>();
        this.network.put(key, getSelf());
        this.writeRequests = new HashSet<>();
    }

    static public Props props(Integer key) {
        return Props.create(Node.class, () -> new Node(key));
    }

    // Dispatcher
    @Override
    public AbstractActor.Receive createReceive() {
        return onlineBehavior();
    }

    private AbstractActor.Receive onlineBehavior() {
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
                .match(Message.LeaveNetworkOrder.class, this::OnLeaveOrder)
                .match(Message.NodeLeaveMsg.class, this::OnNodeLeave)
                .match(Message.PassDataItemsMsg.class, this::OnPassDataItems)
                .match(Message.WriteRequestMsg.class, this::OnWriteRequest)
                .match(Message.ErrorNoValueFound.class, this::OnNoValueFound)
                .match(Message.WriteContentMsg.class, this::OnWriteContentMsg)
                .match(Message.CrashRequestOrder.class, this::OnCrashRequestOrder)
                .match(Message.NetworkRequestMsg.class, this::OnNetworkRequestMsg)
                .match(Message.NetworkResponseMsg.class, this::OnNetworkResponseMsg)
                .match(Message.ErrorMsg.class, this::OnError)
                .match(Message.TimeoutMsg.class, this::OnTimeOut)
                .build();
    }

    // Dispatcher for the crashed node
    private AbstractActor.Receive crashedBehavior() {
        return receiveBuilder()
                .match(Message.RecoveryRequestOrder.class, this::OnRecoveryRequestOrder)
                .match(Message.PrintNode.class, this::OnPrintNode)
                .matchAny(m -> {})  // Ignore all other messages
                .build();
    }

    // New node contacts bootstrapper
    private void OnJoinOrder(Message.JoinNetworkOrder m) {
        if (m != null) {
            isJoining = true;
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
        Integer neighborKey = FindNext();
        ActorRef node = this.network.get(neighborKey);

        // Contact neighbor and request data
        node.tell(new Message.DataRequestMsg(getSelf()), getSelf());
    }

    // Send storage to the new node
    private void OnDataRequest(Message.DataRequestMsg m) {
        m.sender.tell(new Message.DataResponseMsg(this.storage, this.key), getSelf());
    }

    // Node receives the data storage from the neighbor
    private void OnDataResponse(Message.DataResponseMsg m) {
        // Take only necessary data
        Map<Integer, DataEntry> s = m.storage;

        for(Map.Entry<Integer, DataEntry> entry: s.entrySet()) {
            Integer k = entry.getKey();
            DataEntry v = entry.getValue();

            if(IsInInterval(this.key, m.key, k)) {
                this.valuesToCheck++;
                this.storage.put(k, v);
                if(!isRecovering) {
                    getSender().tell(new Message.ReadRequestMsg(getSelf(), k, 0), getSelf());
                }
            }
        }

        if(this.valuesToCheck == 0) {
            Multicast(new Message.NodeAnnounceMsg(this.key), new HashSet<>(this.network.values()));
        }

        if(isRecovering) {
            isRecovering = false;
        }
    }

    // Node that receives a multicast message
    private void OnNodeAnnounce(Message.NodeAnnounceMsg m) {
        this.network.put(m.key, getSender());
        HashSet<Integer> keySet = new HashSet<>(this.storage.keySet());

        for(Integer k : keySet) {
            if(IsInInterval(m.key, this.key, k)) {
                this.storage.remove(k);
            }
        }
    }

    // Node accepts read request
    private void OnReadRequest(Message.ReadRequestMsg m) {
        // TODO: put a timeout and print error if it ends
        if(this.storage.containsKey(m.key)) {
            getSender().tell(new Message.ReadResponseMsg(m.sender, m.key, storage.get(m.key), m.message_id), getSelf());
        } else {
            ActorRef holdingNode = this.network.get(FindResponsible(m.key));
            if(holdingNode == getSelf()) {
                m.sender.tell(new Message.ErrorNoValueFound("No value found for the requested key", m.sender, m.key, null), getSelf());
            } else {
                holdingNode.tell(new Message.ReadRequestMsg(m.sender, m.key, m.message_id), getSelf());
            }
        }
    }

    // Node performs write operation
    private void OnNoValueFound(Message.ErrorNoValueFound m) {

        Message.WriteRequestMsg writeRequest = null;
        for(Message.WriteRequestMsg w : this.writeRequests) {
            if(w.key == m.key && getSelf() == m.readSender) {
                writeRequest = w;
                break;
            }
        }

        if(writeRequest != null) {
            DataEntry data;

            if(m.data == null) {
                data = new DataEntry(writeRequest.value);
            } else {
                data = m.data;
                data.SetValue(writeRequest.value, false);
            }

            writeRequest.sender.tell(new Message.WriteResponseMsg(writeRequest.value), getSelf());
            getSender().tell(new Message.WriteContentMsg(m.key, data), getSelf());
            this.writeRequests.remove(writeRequest);
        } else {
            System.out.println("I've sent a read request for a non existing value and I don't know why");
        }
    }

    // Put the received data in the storage
    private void OnWriteContentMsg(Message.WriteContentMsg m) {
        InsertData(m.key, m.data);
    }

    // Node performs read operation
    private void OnReadResponse(Message.ReadResponseMsg m) {

        // Node is the recipient of the message
        if(m.recipient == getSelf()) {
            // Node is reading to join the network
            if(this.isJoining) {
                // Check data is OK
                this.storage.put(m.key, m.value);
                this.valuesToCheck--;

                if(this.valuesToCheck == 0) {
                    // Node is ready, Multicast to every other nodes in the network
                    Multicast(new Message.NodeAnnounceMsg(this.key), new HashSet<>(this.network.values()));
                }
            } else { // Node is reading to write

                Message.WriteRequestMsg writeRequest = null;
                for(Message.WriteRequestMsg w : this.writeRequests) {
                    //System.out.println("w.key: " + w.key + "; w.sender: " + w.sender + "; m.key: " + m.key + "; m.recipient: " + m.recipient);
                    System.out.println("w.message_id: " + w.message_id + "; m.message_id: " + m.message_id);
                    if(w.message_id == m.message_id) {
                        writeRequest = w;
                        break;
                    }
                }

                if(writeRequest != null) {
                    System.out.println("Fatto! Guarda... la porta si è chiusa");
                    m.value.SetValue(writeRequest.value, true);
                    getSender().tell(new Message.WriteContentMsg(m.key, m.value), getSelf());
                    writeRequest.sender.tell(new Message.WriteResponseMsg(writeRequest.value), getSelf());
                    this.writeRequests.remove(writeRequest);
                }

                //OnNoValueFound(new Message.ErrorNoValueFound("No value found for the requested key", getSelf(), m.key, m.value));
                //DataEntry data = m.value.SetValue();
                //InsertData(m.key, m.value);
                //TODO controlla che versione dato da inserire sia maggiore
            }
        } else {
            // Forward request
            m.recipient.tell(m, getSelf());
        }
    }

    // Node receives the command to leave the network
    private void OnLeaveOrder(Message.LeaveNetworkOrder m) {
        // Multicast everyone
        Multicast(new Message.NodeLeaveMsg(this.key), new HashSet<>(this.network.values()));

        // Get neighbor key
        Integer neighborKey = FindNext();
        if(neighborKey != this.key) {
            ActorRef node = this.network.get(neighborKey);

            // Contact neighbor and pass data items
            node.tell(new Message.PassDataItemsMsg(this.storage), getSelf());
        }
    }

    // Node includes receiving items to its storage
    private void OnPassDataItems(Message.PassDataItemsMsg m) {
        this.storage.putAll(m.storage);
    }

    // Removes the leaving node from the network
    private void OnNodeLeave(Message.NodeLeaveMsg m) {
        Integer k = m.key;
        this.network.remove(k);
    }

    // Print errors
    private void OnError(Message.ErrorMsg m) {
        System.err.println(m.msg);
        System.err.println();
    }

    // Node receives a write request
    private void OnWriteRequest(Message.WriteRequestMsg m) {
        //TODO: put a timeout and print error if it ends
        ActorRef node = this.network.get(FindResponsible(m.key));
        this.writeRequests.add(m);
        // Timeout for the write request
        getContext().system().scheduler().scheduleOnce(
                Duration.create(400, TimeUnit.MILLISECONDS),                    // how frequently generate them
                getSelf(),                                                       // destination actor reference
                new Message.TimeoutMsg(m.sender, m.key, "Write time-out", m.message_id),       // the message to send
                getContext().system().dispatcher(),                              // system dispatcher
                getSelf()                                                        // source of the message (myself)
        );
        node.tell(new Message.ReadRequestMsg(getSelf(), m.key, m.message_id), getSelf());
    }

    // Node receives the order to crash
    private void OnCrashRequestOrder(Message.CrashRequestOrder m) {
        // Change dispatcher and set itself as crashed
        getContext().become(crashedBehavior());
        isCrashed = true;
    }

    // Node receives the order to recovery from the crashed state
    private void OnRecoveryRequestOrder(Message.RecoveryRequestOrder m) {
        if(m.node != null) {
            // Contact bootstrapper node for recovery
            m.node.tell(new Message.NetworkRequestMsg(), getSelf());
        }

        getContext().become(onlineBehavior());
        isCrashed = false;
    }

    // Tell node to begin recovery procedure
    private void OnNetworkRequestMsg(Message.NetworkRequestMsg m) {
        getSender().tell(new Message.NetworkResponseMsg(this.network), getSelf());
    }

    // Node begins recovery protocol by asking other nodes the data items
    private void OnNetworkResponseMsg(Message.NetworkResponseMsg m) {
        isRecovering = true;

        // Forgets items it is no longer responsible for
        HashSet<Integer> keySet = new HashSet<>(this.storage.keySet());

        Integer previousKey = FindPredecessor();
        Integer nextKey = FindNext();

        for(Integer k : keySet) {
            if(IsInInterval(previousKey, this.key, k)) {
                this.storage.remove(k);
            }
        }

        // Request items we are responsible for
        this.network.get(nextKey).tell(new Message.DataRequestMsg(getSelf()), getSelf());
    }

    // Print node storage
    private void OnPrintNode(Message.PrintNode m) {
        if(isCrashed) {
            System.err.println("\t Node: " + this.key);
        }
        else {
            System.out.println("\t Node: " + this.key);
        }

        for(Map.Entry<Integer, DataEntry> entry: this.storage.entrySet()) {
            System.out.println("\t\t" + " Key: " + entry.getKey() + " Value: " + entry.getValue().GetValue() + " Version: " + entry.getValue().GetVersion());
        }

        System.out.println();
    }

    // Node timeout
    private void OnTimeOut(Message.TimeoutMsg m) {

        Message.WriteRequestMsg writeRequest = null;
        for(Message.WriteRequestMsg w : this.writeRequests) {
            System.out.println("PIRLAA w.message_id: " + w.message_id + "; m.message_id: " + m.message_id);
            if(w.message_id == m.message_id) {
                writeRequest = w;
                break;
            }
        }

        if(writeRequest != null) {
            m.recipient.tell(new Message.ErrorMsg("Cannot update value for key: " + m.key), getSelf());
            this.writeRequests.remove(writeRequest);
        }
    }

    // Wrapper
    private Integer FindNext() {
        return FindNext(this.key);
    }

    // Find the node for key k
    private Integer FindNext(Integer k) {
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

    // Find the node responsible for key k
    private Integer FindResponsible(Integer k) {
        if(this.network.containsKey(k)) {
            return k;
        } else {
            return FindNext(k);
        }
    }

    // Wrapper
    private Integer FindPredecessor() {
        return FindPredecessor(this.key);
    }

    // Find the predecessor node with key k
    private Integer FindPredecessor(Integer k) {
        Integer neighborKey;

        Set<Integer> keySet = this.network.keySet();
        ArrayList<Integer> keyList = new ArrayList<>(keySet);
        Collections.sort(keyList);

        for(int i = keyList.size() - 1; i >= 0; i--) {
            if(keyList.get(i) < k) {
                neighborKey = keyList.get(i);
                return neighborKey;
            }
        }

        neighborKey = keyList.get(keyList.size() - 1);
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

    // Put data in the storage
    private void InsertData(Integer key, DataEntry value) {

        // Check if a data item with this key is already in the storage
        if(this.storage.containsKey(key)) {
            if(this.storage.get(key).GetValue() != value.GetValue()) {
                this.storage.get(key).SetValue(value.GetValue(), false);
            }
        } else {
            this.storage.put(key, new DataEntry(value.GetValue()));
        }
    }
}
