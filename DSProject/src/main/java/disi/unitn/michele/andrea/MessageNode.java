package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.util.Collections;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/***** Messages received/sent by a Node *****/
public class MessageNode {

    ////////////////////
    // Join
    public static class JoinSystemMsg implements Serializable {
        public final ActorRef bootstrap_node;

        public JoinSystemMsg(ActorRef bootstrap_node) {
            this.bootstrap_node = bootstrap_node;
        }
    }

    ////////////////////
    // Leave
    public static class LeaveNetworkMsg implements Serializable{}

    public static class NodeLeaveMsg implements Serializable {
        public final int key;

        public NodeLeaveMsg(int key) {
            this.key = key;
        }
    }

    ////////////////////
    // Requests
    public static class GetRequestMsg implements Serializable {
        public final int key;
        public final int msg_id;

        public GetRequestMsg(int key, int msg_id) {
            this.key = key;
            this.msg_id = msg_id;
        }
    }

    public static class ReadRequestMsg implements Serializable {
        public final ActorRef sender;
        public final int key;
        public final int msg_id;
        public final boolean is_write;

        public ReadRequestMsg(ActorRef sender, int key, int msg_id) {
            this.sender = sender;
            this.key = key;
            this.msg_id = msg_id;
            this.is_write = false;
        }

        public ReadRequestMsg(ActorRef sender, int key, int msg_id, boolean is_write) {
            this.sender = sender;
            this.key = key;
            this.msg_id = msg_id;
            this.is_write = is_write;
        }
    }

    public static class UpdateRequestMsg implements Serializable {
        public final int key;
        public final String value;
        public final int msg_id;

        public UpdateRequestMsg(int key, String value, int msg_id) {
            this.key = key;
            this.value = value;
            this.msg_id = msg_id;
        }
    }

    public static class DataRequestMsg implements Serializable {
        public final int key;
        public final int msg_id;

        public DataRequestMsg(int key, int msg_id) {
            this.key = key;
            this.msg_id = msg_id;
        }
    }

    public static class RecoveryRequestMsg implements Serializable {
        public final ActorRef node;

        public RecoveryRequestMsg(ActorRef node){
            this.node = node;
        }
    }

    public static class NetworkRequestMsg implements Serializable {}
    public static class CrashRequestMsg implements Serializable {}

    ////////////////////
    // Responses
    public static class ReadResponseMsg implements Serializable {
        public final ActorRef recipient;
        public final int key;
        public final DataEntry entry;
        public final int msg_id;

        public ReadResponseMsg(ActorRef recipient, int key, DataEntry entry, int msg_id) {
            this.recipient = recipient;
            this.key = key;
            this.entry = new DataEntry(entry.GetValue(), entry.GetVersion());
            this.msg_id = msg_id;
        }
    }

    public static class DataResponseMsg implements Serializable {
        public final Map<Integer, DataEntry> storage;
        public final int key;
        public final int msg_id;

        public DataResponseMsg(HashMap<Integer, DataEntry> storage, int key, int msg_id) {
            this.storage = Collections.unmodifiableMap(storage);
            this.key = key;
            this.msg_id = msg_id;
        }
    }

    public static class NetworkResponseMsg implements Serializable {
        public final Map<Integer, ActorRef> network;

        public NetworkResponseMsg(HashMap<Integer, ActorRef> network) {
            this.network = Collections.unmodifiableMap(network);
        }
    }

    ////////////////////
    // Timeouts
    public static class BaseTimeout {
        public final int key;

        public BaseTimeout(int key) {
            this.key = key;
        }
    }

    public static class ReadTimeoutMsg extends BaseTimeout implements Serializable {
        public final ActorRef recipient;
        public final String msg;
        public final int msg_id;

        public ReadTimeoutMsg(ActorRef recipient, int key, String msg, int msg_id) {
            super(key);

            this.recipient = recipient;
            this.msg = msg;
            this.msg_id = msg_id;
        }
    }

    public static class WriteTimeoutMsg extends BaseTimeout implements Serializable {
        public final ActorRef recipient;
        public final String msg;
        public final int msg_id;

        public WriteTimeoutMsg(ActorRef recipient, int key, String msg, int msg_id) {
            super(key);

            this.recipient = recipient;
            this.msg = msg;
            this.msg_id = msg_id;
        }
    }

    public static class NeighborTimeoutMsg extends BaseTimeout implements Serializable {
        public final ActorRef recipient;
        public final int msg_id;

        public NeighborTimeoutMsg(ActorRef recipient, int key, int msg_id) {
            super(key);

            this.recipient = recipient;
            this.msg_id = msg_id;
        }
    }

    ////////////////////
    // Utility
    public static class NodeAnnounceMsg implements Serializable {
        public final int key;

        public NodeAnnounceMsg(int key) {
            this.key = key;
        }
    }

    public static class ReleaseLockMsg implements Serializable {
        public final int key;
        public final ActorRef client;

        public ReleaseLockMsg(int key, ActorRef client) {
            this.key = key;
            this.client = client;
        }
    }

    public static class PassDataItemsMsg implements Serializable {
        public final Map<Integer, DataEntry> storage;

        public PassDataItemsMsg(HashMap<Integer, DataEntry> storage) {
            this.storage = Collections.unmodifiableMap(storage);
        }
    }

    public static class WriteContentMsg implements Serializable {
        public final int key;
        public final DataEntry entry;

        public WriteContentMsg(int key, DataEntry entry) {
            this.key = key;
            this.entry = new DataEntry(entry.GetValue(), entry.GetVersion());
        }
    }

    public static class PrintNodeMsg implements Serializable {}

    ////////////////////
    // Errors
    public static class ErrorMsg implements Serializable {
        public final String msg;

        public ErrorMsg(String msg) {
            this.msg = msg;
        }
    }

    public static class ErrorNoValueFoundMsg extends ErrorMsg implements Serializable {
        public final ActorRef read_sender;
        public final DataEntry entry;
        public final int key;
        public final int msg_id;

        public ErrorNoValueFoundMsg(String msg, ActorRef read_sender, DataEntry entry, int key, int msg_id) {
            super(msg);

            this.read_sender = read_sender;
            this.entry = new DataEntry(entry.GetValue(), entry.GetVersion());
            this.key = key;
            this.msg_id = msg_id;
        }
    }
}
