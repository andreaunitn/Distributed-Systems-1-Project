package disi.unitn.michele.andrea;

import akka.actor.ActorRef;
import java.io.Serializable;

/***** Messages received by the Client coming from the Main or Nodes *****/
public class MessageClient {
    ////////////////////
    // Join
    public static class JoinSystemMsg implements Serializable {}

    // Requests
    public static class GetRequestMsg implements Serializable {
        public final ActorRef node_coordinator;
        public final int key;

        public GetRequestMsg(ActorRef node_coordinator, int key) {
            this.node_coordinator = node_coordinator;
            this.key = key;
        }
    }

    public static class UpdateRequestMsg implements Serializable {
        public final ActorRef node_coordinator;
        public final int key;
        public final String value;

        public UpdateRequestMsg(ActorRef node_coordinator, int key, String value) {
            this.node_coordinator = node_coordinator;
            this.key = key;
            this.value = value;
        }
    }

    ////////////////////
    // Responses
    public static class GetResponseMsg implements Serializable {
        public final ActorRef recipient_node;
        public final int key;
        public final DataEntry entry;
        public final int msg_id;

        public GetResponseMsg(ActorRef recipient_node, int key, DataEntry entry, int msg_id) {
            this.recipient_node = recipient_node;
            this.key = key;
            this.entry = entry;
            this.msg_id = msg_id;
        }
    }

    public static class UpdateResponseMsg implements Serializable {
        public final String value;
        public final int msg_id;

        public UpdateResponseMsg(final String value, final int msg_id) {
            this.value = value;
            this.msg_id = msg_id;
        }
    }

    ////////////////////
    // Timeouts
    public static class BaseTimeout {
        public final ActorRef recipient;
        public final int key;
        public final String msg;
        public final int msg_id;

        public BaseTimeout(ActorRef recipient, int key, String msg, int msg_id) {
            this.recipient = recipient;
            this.key = key;
            this.msg = msg;
            this.msg_id = msg_id;
        }
    }

    public static class GetTimeoutMsg extends BaseTimeout implements Serializable {
        public GetTimeoutMsg(ActorRef recipient, int key, String msg, int msg_id) {
            super(recipient, key, msg, msg_id);
        }
    }

    public static class UpdateTimeoutMsg extends BaseTimeout implements Serializable {
        public UpdateTimeoutMsg(ActorRef recipient, int key, String msg, int msg_id) {
            super(recipient, key, msg, msg_id);
        }
    }

    ////////////////////
    // Log
    public static class PrintSelfMsg implements Serializable {}

    public static class PrintErrorMsg implements Serializable {
        public final String msg;

        public PrintErrorMsg(String msg) {
            this.msg = msg;
        }
    }
}
