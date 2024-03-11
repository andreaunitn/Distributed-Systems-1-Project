package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/***** Messages received by a Node coming from the Main or Client *****/
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
    // Requests
    public static class GetRequestMsg implements Serializable {
        public final int key;
        public final int msg_id;

        public GetRequestMsg(int key, int msg_id) {
            this.key = key;
            this.msg_id = msg_id;
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

    public static class NetworkRequestMsg implements Serializable {}

    ////////////////////
    // Responses
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

    public static class PassDataTimeoutMsg extends BaseTimeout implements Serializable {
        public PassDataTimeoutMsg(Integer key) {
            super(key);
        }
    }

    public static class NeighborTimeoutMsg extends BaseTimeout implements Serializable {
        public final ActorRef recipient;
        public final int message_id;

        public NeighborTimeoutMsg(ActorRef recipient, int key, int message_id) {
            super(key);
            this.recipient = recipient;
            this.message_id = message_id;
        }
    }

    ////////////////////
    // Utility
}
