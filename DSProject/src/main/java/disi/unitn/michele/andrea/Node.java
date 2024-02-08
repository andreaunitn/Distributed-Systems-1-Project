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
    private boolean canDie = false;

    private HashMap<Integer, ActorRef> network;
    private HashMap<Integer, DataEntry> storage;
    private HashSet<Message.WriteRequestMsg> writeRequests;
    private HashSet<Message.ReadRequestMsg> readRequests;
    private HashSet<Message.DataRequestMsg> dataRequests;

    public Node(Integer key) {
        this.key = key;
        this.rnd = new Random();
        this.storage = new HashMap<>();
        this.network = new HashMap<>();
        this.network.put(key, getSelf());
        this.writeRequests = new HashSet<>();
        this.readRequests = new HashSet<>();
        this.dataRequests = new HashSet<>();
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
                .match(Message.NeighborTimeoutMsg.class, this::OnNeighborTimeout)
                .match(Message.PassDataTimeoutMsg.class, this::OnPassDataTimeoutMsg)
                .match(Message.PassDataItemResponseMsg.class, this::OnPassDataItemResponseMsg)
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
        Message.DataRequestMsg newMsg = new Message.DataRequestMsg(getSelf());

        getContext().system().scheduler().scheduleOnce(
                Duration.create(200, TimeUnit.MILLISECONDS),                    // how frequently generate them
                getSelf(),                                                       // destination actor reference
                new Message.NeighborTimeoutMsg(m.sender, node, neighborKey, newMsg.message_id),       // the message to send
                getContext().system().dispatcher(),                              // system dispatcher
                getSelf()                                                        // source of the message (myself)
        );
        this.dataRequests.add(newMsg);

        // Contact neighbor and request data
        node.tell(newMsg, getSelf());
    }

    // Send storage to the new node
    private void OnDataRequest(Message.DataRequestMsg m) {
        m.sender.tell(new Message.DataResponseMsg(this.storage, this.key, m.message_id), getSelf());
    }

    // Node receives the data storage from the neighbor
    private void OnDataResponse(Message.DataResponseMsg m) {
        System.out.println("OnDataResponse. " + "m.message_id: " + m.message_id);
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

        if(this.isJoining) {
            System.out.println("sender: " + getSender());
            Message.DataRequestMsg dataRequest = null;
            for(Message.DataRequestMsg d: this.dataRequests) {
                if(d.message_id == m.message_id) {
                    dataRequest = d;
                    break;
                }
            }

            if(dataRequest != null) {
                this.dataRequests.remove(dataRequest);
            }
        }

        if(this.valuesToCheck == 0) {
            Multicast(new Message.NodeAnnounceMsg(this.key), new HashSet<>(this.network.values()));
            this.isJoining = false;
        }

        if(this.isRecovering) {
            Message.DataRequestMsg dataRequest = null;
            for(Message.DataRequestMsg d: this.dataRequests) {
                if(d.message_id == m.message_id) {
                    dataRequest = d;
                    break;
                }
            }

            if(dataRequest != null) {
                this.dataRequests.remove(dataRequest);
                this.isRecovering = false;
                getSender().tell(new Message.NodeAnnounceMsg(this.key), getSelf());
            }
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
                m.sender.tell(new Message.ErrorNoValueFound("No value found for the requested key", m.sender, m.key, null, m.message_id), getSelf());
            } else {
                this.readRequests.add(m);

                getContext().system().scheduler().scheduleOnce(
                        Duration.create(200, TimeUnit.MILLISECONDS),                    // how frequently generate them
                        getSelf(),                                                       // destination actor reference
                        new Message.TimeoutMsg(m.sender, m.key, "Read time-out", m.message_id, "read"),       // the message to send
                        getContext().system().dispatcher(),                              // system dispatcher
                        getSelf()                                                        // source of the message (myself)
                );

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

            writeRequest.sender.tell(new Message.WriteResponseMsg(writeRequest.value, m.message_id), getSelf());
            getSender().tell(new Message.WriteContentMsg(m.key, data), getSelf());
            this.writeRequests.remove(writeRequest);
            System.out.println(getSelf() + "Removed in OnNoValueFound");
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

        //System.out.println("Node: " + getSelf() + " ReadResponse: " + m.recipient + " " + m.key + " " + m.value + " " + m.message_id);
        // Node is the recipient of the message
        if(m.recipient == getSelf()) {
            System.out.println(this.isJoining);
            System.out.println(this.valuesToCheck);
            // Node is reading to join the network
            if(this.isJoining) {
                // Check data is OK
                this.storage.put(m.key, m.value);
                if(this.valuesToCheck > 0) {
                    this.valuesToCheck--;
                }

                if(this.valuesToCheck == 0) {
                    System.out.println("valuesToCheck == 0");
                    // Node is ready, Multicast to every other nodes in the network
                    Multicast(new Message.NodeAnnounceMsg(this.key), new HashSet<>(this.network.values()));
                    this.isJoining = false;
                }
            } else { // Node is ready to write

                Message.WriteRequestMsg writeRequest = null;
                //System.out.println("is writeRequests Empty: " + writeRequests.isEmpty());
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
                    writeRequest.sender.tell(new Message.WriteResponseMsg(writeRequest.value, m.message_id), getSelf());
                    this.writeRequests.remove(writeRequest);
                    System.out.println(getSelf() + "Removed in OnReadResponse");
                }

                //OnNoValueFound(new Message.ErrorNoValueFound("No value found for the requested key", getSelf(), m.key, m.value));
                //DataEntry data = m.value.SetValue();
                //InsertData(m.key, m.value);
                //TODO controlla che versione dato da inserire sia maggiore
            }
        } else {
            Message.ReadRequestMsg readRequest = null;
            for(Message.ReadRequestMsg r: this.readRequests) {
                System.out.println("w.message_id: " + r.message_id + "; m.message_id: " + m.message_id);
                if(r.message_id == m.message_id) {
                    readRequest = r;
                    break;
                }
            }

            if(readRequest != null) {
                this.readRequests.remove(readRequest);
            }

            // Forward request
            m.recipient.tell(m, getSelf());
        }
    }

    // Node receives the command to leave the network
    private void OnLeaveOrder(Message.LeaveNetworkOrder m) {

        //TODO trova il modo di mantenere nella rete i dati del nodo uscente quando il successivo è in crash
        //(timeout + diamo al next disponibile, tutti i nodi che entrano di conseguenza dovranno cercare nel primo nodo next in vita)

        // Get neighbor key
        Integer neighborKey = FindNext();
        if(neighborKey != this.key) {
            ActorRef node = this.network.get(neighborKey);

            // Contact neighbor and pass data items
            node.tell(new Message.PassDataItemsMsg(this.storage), getSelf());
        }

        getContext().system().scheduler().scheduleOnce(
                Duration.create(200, TimeUnit.MILLISECONDS),                    // how frequently generate them
                getSelf(),                                                       // destination actor reference
                new Message.PassDataTimeoutMsg(neighborKey),       // the message to send
                getContext().system().dispatcher(),                              // system dispatcher
                getSelf()                                                        // source of the message (myself)
        );
    }

    // Node includes receiving items to its storage
    private void OnPassDataItems(Message.PassDataItemsMsg m) {
        this.storage.putAll(m.storage);
        getSender().tell(new Message.PassDataItemResponseMsg(this.key), getSelf());
    }

    private void OnPassDataItemResponseMsg(Message.PassDataItemResponseMsg m) {
        // Multicast everyone
        Multicast(new Message.NodeLeaveMsg(this.key), new HashSet<>(this.network.values()));
        this.canDie = true;
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
        //System.out.println("Node " + getSelf() + " added this to writeRequests: " + m.message_id);
        // Timeout for the write request
        getContext().system().scheduler().scheduleOnce(
                Duration.create(200, TimeUnit.MILLISECONDS),                    // how frequently generate them
                getSelf(),                                                       // destination actor reference
                new Message.TimeoutMsg(m.sender, m.key, "Write time-out", m.message_id, "write"),       // the message to send
                getContext().system().dispatcher(),                              // system dispatcher
                getSelf()                                                        // source of the message (myself)
        );
        node.tell(new Message.ReadRequestMsg(getSelf(), m.key, m.message_id), getSelf());
    }

    // Node receives the order to crash
    private void OnCrashRequestOrder(Message.CrashRequestOrder m) {
        // Change dispatcher and set itself as crashed
        getContext().become(crashedBehavior());
        this.isCrashed = true;
    }

    // Node receives the order to recovery from the crashed state
    private void OnRecoveryRequestOrder(Message.RecoveryRequestOrder m) {
        if(m.node != null) {
            // Contact bootstrapper node for recovery
            m.node.tell(new Message.NetworkRequestMsg(), getSelf());
        }

        getContext().become(onlineBehavior());
        this.isCrashed = false;
    }

    // Tell node to begin recovery procedure
    private void OnNetworkRequestMsg(Message.NetworkRequestMsg m) {
        getSender().tell(new Message.NetworkResponseMsg(this.network), getSelf());
    }

    // Node begins recovery protocol by asking other nodes the data items
    private void OnNetworkResponseMsg(Message.NetworkResponseMsg m) {
        this.isRecovering = true;

        // Update network
        this.network.clear();
        this.network.putAll(m.network);

        // Forgets items it is no longer responsible for
        HashSet<Integer> keySet = new HashSet<>(this.storage.keySet());

        Integer previousKey = FindPredecessor();
        Integer nextKey = FindNext();

        //TODO aggiusta il check qui sotto, invia i dati che non sono più sotto la mia responsabilità prima di eliminarli
        for(Integer k : keySet) {
            if(IsInInterval(previousKey, this.key, k)) {
                this.storage.remove(k);
            }
        }

        System.out.println("OnNeighborTimeout. sender: " + getSelf() + ", recipient: " + nextKey);
        Message.DataRequestMsg dataRequest = new Message.DataRequestMsg(getSelf());
        getContext().system().scheduler().scheduleOnce(
                Duration.create(200, TimeUnit.MILLISECONDS),                    // how frequently generate them
                getSelf(),                                                       // destination actor reference
                new Message.NeighborTimeoutMsg(getSelf(), this.network.get(nextKey), nextKey, dataRequest.message_id),       // the message to send
                getContext().system().dispatcher(),                              // system dispatcher
                getSelf()                                                        // source of the message (myself)
        );

        this.dataRequests.add(dataRequest);
        System.out.println("Creating timer " + dataRequest.message_id);
        // Request items we are responsible for
        this.network.get(nextKey).tell(dataRequest, getSelf());
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
        if(m.operation.equals("write")) {
            Message.WriteRequestMsg writeRequest = null;
            for(Message.WriteRequestMsg w : this.writeRequests) {
                System.out.println("Timeout w.message_id: " + w.message_id + "; m.message_id: " + m.message_id);
                if(w.message_id == m.message_id) {
                    writeRequest = w;
                    break;
                }
            }

            if(writeRequest != null) {
                m.recipient.tell(new Message.ErrorMsg("Cannot update value for key: " + m.key), getSelf());
                this.writeRequests.remove(writeRequest);
                System.out.println(getSelf() + "Removed writeRequest in OnTimeOut");
            }
        } else if(m.operation.equals("read")) {
            Message.ReadRequestMsg readRequest = null;
            for(Message.ReadRequestMsg r: this.readRequests) {
                System.out.println("Timeout w.message_id: " + r.message_id + "; m.message_id: " + m.message_id);
                if(r.message_id == m.message_id) {
                    readRequest = r;
                    break;
                }
            }

            if(readRequest != null) {
                m.recipient.tell(new Message.ErrorMsg("Cannot read value for key: " + m.key), getSelf());
                this.readRequests.remove(readRequest);
                System.out.println(getSelf() + "Removed readRequest in OnTimeOut");
            }
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

    public void OnNeighborTimeout(Message.NeighborTimeoutMsg m) {

        Message.DataRequestMsg dataRequest = null;
        for(Message.DataRequestMsg d: this.dataRequests) {
            if(d.message_id == m.message_id) {
                dataRequest = d;
                break;
            }
        }

        if(dataRequest != null) {
            this.dataRequests.remove(dataRequest);

            ActorRef neighbor = this.network.get(FindNext(m.key));

            System.out.println("OnNeighborTimeout. sender: " + getSelf() + ", recipient: " + neighbor);
            Message.DataRequestMsg newDataRequest = new Message.DataRequestMsg(getSelf());

            if(neighbor != getSelf()) {
                getContext().system().scheduler().scheduleOnce(
                        Duration.create(200, TimeUnit.MILLISECONDS),                    // how frequently generate them
                        getSelf(),                                                       // destination actor reference
                        new Message.NeighborTimeoutMsg(getSelf(), neighbor, FindNext(m.key), newDataRequest.message_id),       // the message to send
                        getContext().system().dispatcher(),                              // system dispatcher
                        getSelf()                                                        // source of the message (myself)
                );

                this.dataRequests.add(newDataRequest);
                // Contact next neighbor if the first one is crashed
                neighbor.tell(newDataRequest, getSelf());
            } else {
                System.out.println("There are no other nodes alive in the network");
            }
        }
    }

    public void OnPassDataTimeoutMsg(Message.PassDataTimeoutMsg m) {

        if(this.canDie) {
            getSelf().tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
        }
        else {
            // Get neighbor key
            Integer neighborKey = FindNext(m.key);
            if(neighborKey != this.key) {
                ActorRef node = this.network.get(neighborKey);

                // Contact neighbor and pass data items
                node.tell(new Message.PassDataItemsMsg(this.storage), getSelf());
            }

            getContext().system().scheduler().scheduleOnce(
                    Duration.create(200, TimeUnit.MILLISECONDS),                    // how frequently generate them
                    getSelf(),                                                       // destination actor reference
                    new Message.PassDataTimeoutMsg(neighborKey),       // the message to send
                    getContext().system().dispatcher(),                              // system dispatcher
                    getSelf()                                                        // source of the message (myself)
            );
        }
    }
}
