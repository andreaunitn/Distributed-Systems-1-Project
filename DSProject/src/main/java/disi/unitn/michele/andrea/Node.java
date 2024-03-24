package disi.unitn.michele.andrea;

import akka.actor.PoisonPill;
import scala.concurrent.duration.Duration;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {

    private static class Identifier {
        Integer id;
        ActorRef client;
        Identifier(Integer id, ActorRef client) {
            this.id = id;
            this.client = client;
        }
    }

    private final Random rnd;
    private final Integer key;
    private boolean isCrashed = false;
    private boolean isJoining = false;
    private boolean isRecovering = false;
    private int valuesToCheck = 0;
    private boolean canDie = false;

    // Counter to be used for message ids. Gets increased at every use
    private int counter = 0;

    // Network view
    private HashMap<Integer, ActorRef> network;

    // Data contained
    private HashMap<Integer, DataEntry> storage;
    
    // Contains the associations between the message id and the originating client for the write requests
    private HashMap<Integer, Identifier> write_requests;

    // Contains the association between the request id and the originating client for the read requests
    private HashMap<Integer, Identifier> read_requests;

    // Contains the association between the message id and the read responses (used to hold messages for replication)
    private HashMap<Integer, List<DataEntry>> read_responses;

    // Contains the association between the message id and the value to be updated
    private HashMap<Integer, String> update_values;
    
    // Contains the ids of sent data requests of which I still haven't received an answer
    private HashSet<Integer> data_requests;

    // Values for replication: N=number of copies of some data, W=write quorum, R=read quorum
    private int N;
    private int R;
    private int W;

    // Timeout value in ms
    private int T;

    public Node(Integer key, int N, int R, int W, int T) {
        this.key = key;
        this.rnd = new Random();
        this.storage = new HashMap<>();
        this.network = new HashMap<>();
        this.network.put(key, getSelf());
        this.write_requests = new HashMap<>();
        this.read_requests = new HashMap<>();
        this.read_responses = new HashMap<>();
        this.data_requests = new HashSet<>();
        this.update_values = new HashMap<>();
        this.N = N;
        this.R = R;
        this.W = W;
        this.T = T;
    }

    static public Props props(Integer key, int N, int R, int W, int T) {
        return Props.create(Node.class, () -> new Node(key, N, R, W, T));
    }

    // Dispatcher
    @Override
    public Receive createReceive() {
        return onlineBehavior();
    }

    // Dispatcher for the operating node
    private Receive onlineBehavior() {
        return receiveBuilder()
                .match(Message.PrintNode.class, this::OnPrintNode)
                .match(Message.NodeAnnounceMsg.class, this::OnNodeAnnounce)
                .match(Message.ReadRequestMsg.class, this::OnReadRequest)
                .match(Message.ReadResponseMsg.class, this::OnReadResponse)
                .match(Message.LeaveNetworkOrder.class, this::OnLeaveOrder)
                .match(Message.NodeLeaveMsg.class, this::OnNodeLeave)
                .match(Message.PassDataItemsMsg.class, this::OnPassDataItems)
                .match(Message.ErrorNoValueFound.class, this::OnNoValueFound)
                .match(Message.WriteContentMsg.class, this::OnWriteContentMsg)
                .match(Message.CrashRequestOrder.class, this::OnCrashRequestOrder)
                //.match(Message.NetworkResponseMsg.class, this::OnNetworkResponseMsg)
                .match(Message.ErrorMsg.class, this::OnError)
                .match(Message.PassDataItemResponseMsg.class, this::OnPassDataItemResponseMsg)

                // New
                // Join
                .match(MessageNode.JoinSystemMsg.class, this::OnJoinSystem)

                // Requests
                .match(MessageNode.GetRequestMsg.class, this::OnGetRequest)
                .match(MessageNode.UpdateRequestMsg.class, this::OnUpdateRequest)
                .match(MessageNode.NetworkRequestMsg.class, this::OnNetworkRequest)
                .match(MessageNode.DataRequestMsg.class, this::OnDataRequest)

                // Responses
                .match(MessageNode.DataResponseMsg.class, this::OnDataResponse)
                .match(MessageNode.NetworkResponseMsg.class, this::OnNetworkResponse)

                // Timeouts
                .match(MessageNode.ReadTimeoutMsg.class, this::OnReadTimeout)
                .match(MessageNode.WriteTimeoutMsg.class, this::OnWriteTimeout)
                .match(MessageNode.NeighborTimeoutMsg.class, this::OnNeighborTimeout)
                .match(MessageNode.PassDataTimeoutMsg.class, this::OnPassDataTimeoutMsg)

                .build();
    }

    // Dispatcher for the crashed node
    private Receive crashedBehavior() {
        return receiveBuilder()
                .match(Message.RecoveryRequestOrder.class, this::OnRecoveryRequestOrder)
                .match(Message.PrintNode.class, this::OnPrintNode)
                .matchAny(m -> {})  // Ignore all other messages
                .build();
    }

    // New node contacts bootstrapper
    private void OnJoinSystem(MessageNode.JoinSystemMsg m) {
        this.isJoining = true;
        m.bootstrap_node.tell(new MessageNode.NetworkRequestMsg(), getSelf());
    }

    private void OnDataRequest(MessageNode.DataRequestMsg m) {

        // Filter data to be sent to the node
        HashMap<Integer, DataEntry> data_to_be_sent = new HashMap<>();

        for(Map.Entry<Integer, DataEntry> entry: this.storage.entrySet()) {
            Integer k = entry.getKey();
            DataEntry v = entry.getValue();

            if(IsInInterval(m.key, this.key, k)) {
                data_to_be_sent.put(k, v);
            }
        }

        // Send response
        getSender().tell(new MessageNode.DataResponseMsg(data_to_be_sent, this.key, m.msg_id), getSelf());
    }

    private void OnDataResponse(MessageNode.DataResponseMsg m) {

        // Inserting received data into the storage and performing a read to check that data is up-to-date
        this.valuesToCheck += m.storage.size();
        this.storage.putAll(m.storage);

        for(Integer k: m.storage.keySet()) {
            if(!this.isRecovering) {
                getSender().tell(new Message.ReadRequestMsg(getSelf(), k, 0), getSelf());
            }
        }

        // Joining Phase
        if(this.isJoining) {

            // Request removed because is completed
            this.data_requests.remove(m.msg_id);

            // The node has sent an empty storage and can join the network
            if(this.valuesToCheck == 0) {
                Multicast(new Message.NodeAnnounceMsg(this.key), new HashSet<>(this.network.values()));
                this.isJoining = false;
            }
        }

        // Recovering phase
        if(this.isRecovering) {

            boolean foundAndRemoved = this.data_requests.remove(m.msg_id);
            if(foundAndRemoved) {
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


    // TODO: ripartisci oppurtonamente i compiti di OnReadRequest e OnGetRequest
    private void OnGetRequest(MessageNode.GetRequestMsg m) {

        this.read_requests.put(this.counter, new Identifier(m.msg_id, getSender()));
        this.read_responses.put(this.counter, new ArrayList<DataEntry>());

        // Contact the nodes responsible for the key
        if(network.size() < N) {
            ActorRef holdingNode = this.network.get(FindResponsible(m.key));
            holdingNode.tell(new Message.ReadRequestMsg(getSender(), m.key, this.counter), getSelf());
        } else {
            for(ActorRef node : FindResponsibles(m.key)) {
                node.tell(new Message.ReadRequestMsg(getSelf(), m.key, this.counter), getSelf());
            }
        }

        // Timeout
        SetTimeout(new MessageNode.ReadTimeoutMsg(getSender(), m.key, "Read time-out", this.counter));
        this.counter += 1;
    }

    // Node accepts read request
    private void OnReadRequest(Message.ReadRequestMsg m) {
        if(this.storage.containsKey(m.key)) {
            getSender().tell(new Message.ReadResponseMsg(m.sender, m.key, storage.get(m.key), m.message_id), getSelf());
        } else {
            getSender().tell(new Message.ErrorNoValueFound("No value found for the requested key", m.sender, m.key, null, m.message_id), getSelf());
        }
    }

    // Node performs write operation or read goes wrong
    private void OnNoValueFound(Message.ErrorNoValueFound m) {

        Identifier identity = this.read_requests.get(m.message_id);
        if(identity != null) {
            identity.client.tell(new MessageClient.PrintErrorMsg("cannot read value for key: " + m.key), getSelf());
            this.read_requests.remove(m.message_id);
            this.read_responses.remove(m.message_id);
        }
        else {
            identity = this.write_requests.get(m.message_id);
            ActorRef recipient = identity.client;
            String newValue = this.update_values.get(m.message_id);

            DataEntry data = new DataEntry(newValue);

            recipient.tell(new MessageClient.UpdateResponseMsg(newValue, identity.id), getSelf());
            getSender().tell(new Message.WriteContentMsg(m.key, data), getSelf());

            // removing request because is completed
            this.write_requests.remove(m.message_id);

            // removing value because the update is completed
            this.update_values.remove(m.message_id);
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

            // Node is ready to join the network
            if(this.isJoining) {

                // Check data is OK
                this.storage.put(m.key, m.value);

                if(this.valuesToCheck > 0) {
                    this.valuesToCheck--;
                }

                if(this.valuesToCheck == 0) {

                    // Node is ready, Multicast to every other nodes in the network
                    Multicast(new Message.NodeAnnounceMsg(this.key), new HashSet<>(this.network.values()));
                    this.isJoining = false;
                }

            } else { // Node is ready to write

                // this node is the recipient
                // we're deleting the write request from the relative map

                // deleting write request from map
                Identifier identity = this.write_requests.get(m.message_id);
                ActorRef recipient = identity.client;
                String newValue = this.update_values.get(m.message_id);

                m.value.SetValue(newValue, true);
                getSender().tell(new Message.WriteContentMsg(m.key, m.value), getSelf());
                recipient.tell(new MessageClient.UpdateResponseMsg(newValue, identity.id), getSelf());

                // removing request because is completed
                this.write_requests.remove(m.message_id);

                // removing value because the update is completed
                this.update_values.remove(m.message_id);

                // TODO controlla che versione dato da inserire sia maggiore (per la replication)
            }
        } else {
            // this node isn't the recipient but the client is
            // we're forwarding the message and deleting the read request from the relative map

            // check if the available data is newer, in that case ignore (delete) other messages
            // if older, don't add it to list
            int listSize = this.read_responses.get(m.message_id).size();
            for (DataEntry entry : this.read_responses.get(m.message_id)) {
                if(entry.GetVersion() < m.value.GetVersion()) {
                    this.read_responses.get(m.message_id).remove(entry);
                }
            }
            if (listSize <= this.read_responses.get(m.message_id).size()) {
                this.read_responses.get(m.message_id).add(m.value);
            }

            // check if we have enough responses
            if(this.read_responses.get(m.message_id).size() == this.R) {
                // we have enough responses, we can forward the response to the client
                Identifier identity = this.read_requests.get(m.message_id);
                ActorRef recipient = identity.client;
                DataEntry value = this.read_responses.get(m.message_id).get(0);
                recipient.tell(new MessageClient.GetResponseMsg(recipient, m.key, value, identity.id), getSelf());
                this.read_requests.remove(m.message_id);
                this.read_responses.remove(m.message_id);
            }
        }
    }

    // Node receives the command to leave the network
    private void OnLeaveOrder(Message.LeaveNetworkOrder m) {

        // Get neighbor key
        Integer neighborKey = FindNext();

        if(neighborKey != this.key) {
            ActorRef node = this.network.get(neighborKey);

            // Contact neighbor and pass data items
            node.tell(new Message.PassDataItemsMsg(this.storage), getSelf());
        }

        // SetTimeout
        SetTimeout(new MessageNode.PassDataTimeoutMsg(neighborKey));
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
    private void OnUpdateRequest(MessageNode.UpdateRequestMsg m) {

        ActorRef node = this.network.get(FindResponsible(m.key));
        Identifier identifier = new Identifier(m.msg_id, getSender());
        this.write_requests.put(this.counter, identifier);
        this.update_values.put(this.counter, m.value);

        // Set timeout
        SetTimeout(new MessageNode.WriteTimeoutMsg(getSender(), m.key, "Write time-out", this.counter));

        node.tell(new Message.ReadRequestMsg(getSelf(), m.key, this.counter), getSelf());
        this.counter += 1;
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
            m.node.tell(new MessageNode.NetworkRequestMsg(), getSelf());
        }

        getContext().become(onlineBehavior());
        this.isCrashed = false;
        this.isRecovering = true;
    }

    // Tell node to begin recovery procedure
    private void OnNetworkRequest(MessageNode.NetworkRequestMsg m) {
        getSender().tell(new MessageNode.NetworkResponseMsg(this.network), getSelf());
    }

    // Node begins recovery protocol by asking other nodes the data items
    private void OnNetworkResponse(MessageNode.NetworkResponseMsg m) {

        // Send the network to the joining node
        if(this.isJoining) {
            // Update knowledge of the network
            Map<Integer, ActorRef> received_network = m.network;
            this.network.putAll(received_network);

            // Find neighbor
            Integer neighborKey = FindNext();
            ActorRef node = this.network.get(neighborKey);

            // Creating a new data request
            MessageNode.DataRequestMsg data_request = new MessageNode.DataRequestMsg(this.key, this.counter);
            this.counter += 1;
            this.data_requests.add(data_request.msg_id);

            // Set timeout
            SetTimeout(new MessageNode.NeighborTimeoutMsg(node, neighborKey, data_request.msg_id));

            // Contact neighbor and request data
            node.tell(data_request, getSelf());

        } else if(this.isRecovering) { // Send the network to the node that wants to recover

            // Update my network
            this.network.clear();
            this.network.putAll(m.network);

            // Forgets items it is no longer responsible for
            HashSet<Integer> keySet = new HashSet<>(this.storage.keySet());

            Integer previousKey = FindPredecessor();
            Integer nextKey = FindNext();

            //TODO aggiusta il check qui sotto, invia i dati che non sono più sotto la mia responsabilità prima di eliminarli (must be resolved hopefully with replication)
            for(Integer k : keySet) {
                if(IsInInterval(previousKey, this.key, k)) {
                    this.storage.remove(k);
                }
            }

            // Creating a new data request
            MessageNode.DataRequestMsg data_request = new MessageNode.DataRequestMsg(this.key, this.counter);
            this.counter += 1;
            this.data_requests.add(data_request.msg_id);

            // Set timeout
            SetTimeout(new MessageNode.NeighborTimeoutMsg(this.network.get(nextKey), nextKey, data_request.msg_id));

            // Request items we are responsible for
            this.network.get(nextKey).tell(data_request, getSelf());
        }
    }

    // Print node storage
    private void OnPrintNode(Message.PrintNode m) {
        if(this.isCrashed) {
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

    ////////////////////
    // Timeouts

    private void OnReadTimeout(MessageNode.ReadTimeoutMsg m) {

        // if read operation failed, we're removing it from the map and reporting the problem to client
        Identifier identity = this.read_requests.get(m.msg_id);

        if(identity != null) {
            ActorRef recipient = identity.client;
            this.read_requests.remove(m.msg_id);
            this.read_responses.remove(m.msg_id);
            recipient.tell(new MessageClient.PrintErrorMsg("Cannot read value for key: " + m.key), getSelf());
        }
    }

    private void OnWriteTimeout(MessageNode.WriteTimeoutMsg m) {

        // if write operation failed, we're removing it from the map and reporting the problem to client
        Identifier identity = this.write_requests.get(m.msg_id);

        if(identity != null) {
            ActorRef recipient = identity.client;
            this.write_requests.remove(m.msg_id);
            recipient.tell(new MessageClient.PrintErrorMsg("Cannot update value for key: " + m.key), getSelf());
        }
    }
    
    ////////////////////
    // Functions to find the correct node to which send data
    
    // Find the node for key k
    private Integer FindNext(Integer k) {
        Integer neighborKey;

        Set<Integer> keySet = this.network.keySet();
        ArrayList<Integer> keyList = new ArrayList<>(keySet);
        Collections.sort(keyList);

        for (Integer integer : keyList) {
            if (integer > k) {
                neighborKey = integer;
                return neighborKey;
            }
        }

        // No bigger key found, take the first (circular ring)
        neighborKey = keyList.get(0);
        return neighborKey;
    }

    // Wrapper
    private Integer FindNext() {
        return FindNext(this.key);
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

    // Wrapper
    private Integer FindPredecessor() {
        return FindPredecessor(this.key);
    }

    // Find the node responsible for key k
    private Integer FindResponsible(Integer k) {
        if(this.network.containsKey(k)) {
            return k;
        } else {
            return FindNext(k);
        }
    }

    // Find the nodes responsible for key k, use only if network size >= N
    private ActorRef[] FindResponsibles(Integer k) {
        ActorRef[] responsibles = new ActorRef[N];
        responsibles[0] = this.network.get(FindResponsible(k));
        int next = FindNext(k);
        for (int i=1; i<N; i++) {
            responsibles[i] = this.network.get(FindResponsible(next));
            next = FindNext(next);
        }
        return responsibles;
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
            return value <= newNodeKey && value > nextNodeKey;
        } else {
            return value > nextNodeKey || value <= newNodeKey;
        }
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

    public void OnNeighborTimeout(MessageNode.NeighborTimeoutMsg m) {

        boolean foundAndRemoved = this.data_requests.remove(m.message_id);
        if(foundAndRemoved) {
            ActorRef neighbor = this.network.get(FindNext(m.key));

            // Creating a new data request
            MessageNode.DataRequestMsg data_request = new MessageNode.DataRequestMsg(this.key, this.counter);
            this.counter += 1;

            if(neighbor != getSelf()) {

                // Timeout
                SetTimeout(new MessageNode.NeighborTimeoutMsg(neighbor, FindNext(m.key), data_request.msg_id));

                this.data_requests.add(data_request.msg_id);

                // Contact next neighbor if the first one is crashed
                neighbor.tell(data_request, getSelf());

            } else {
                System.out.println("There are no other nodes alive in the network");
            }
        }
    }

    public void OnPassDataTimeoutMsg(MessageNode.PassDataTimeoutMsg m) {

        if(this.canDie) {
            getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());

        } else {

            // Get neighbor key
            Integer neighborKey = FindNext(m.key);

            if(neighborKey != this.key) {
                ActorRef node = this.network.get(neighborKey);

                // Contact neighbor and pass data items
                node.tell(new Message.PassDataItemsMsg(this.storage), getSelf());
            }

            // Timeout
            SetTimeout(new MessageNode.PassDataTimeoutMsg(neighborKey));
        }
    }

    private void SetTimeout(MessageNode.BaseTimeout m) {
        SetTimeout(m, T);
    }

    private void SetTimeout(MessageNode.BaseTimeout m, int msTimer) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(msTimer, TimeUnit.MILLISECONDS),                                                       // how frequently generate them
                getSelf(),                                                                                             // destination actor reference
                m,                                                                                                     // the message to send
                getContext().system().dispatcher(),                                                                    // system dispatcher
                getSelf()                                                                                              // source of the message (myself)
        );
    }
}
