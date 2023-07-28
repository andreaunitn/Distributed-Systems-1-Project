package disi.unitn.michele.andrea;

import akka.actor.Actor;
import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Message {

    //Main
    public static class JoinNetworkOrder implements Serializable {
        public final ActorRef bootstrapNode;
        public JoinNetworkOrder(ActorRef node) {
            this.bootstrapNode = node;
        }
    }

    //Nodes
    public static class JoinRequestMsg implements Serializable {
        public final Integer id;
        public final ActorRef sender;
        public JoinRequestMsg(Integer key, ActorRef node) {
            this.id = key;
            this.sender = node;
        }
    }

    public static class JoinResponseMsg implements Serializable {
        public final Map<Integer, ActorRef> network;
        public final ActorRef sender;
        public JoinResponseMsg(HashMap<Integer, ActorRef> network, ActorRef sender) {
            this.network = Collections.unmodifiableMap(network);
            this.sender = sender;
        }
    }

    public static class DataRequestMsg implements Serializable {
        public final ActorRef sender;
        public DataRequestMsg(ActorRef sender) {
            this.sender = sender;
        }
    }

    public static class DataResponseMsg implements Serializable {
        public final Map<Integer, String> storage;
        public final Integer key;
        public DataResponseMsg(HashMap<Integer, String> storage, Integer key) {
            this.storage = Collections.unmodifiableMap(storage);
            this.key = key;
        }
    }

    public static class NodeAnnounceMsg implements Serializable {
        public final Integer key;
        public NodeAnnounceMsg(Integer key) {
            this.key = key;
        }
    }

    public static class PrintNode implements Serializable {
        public PrintNode () {}
    }

    public static class PrintClient implements Serializable {
        public PrintClient() {}
    }

    // Node + Client
    public static class ReadRequestMsg implements Serializable {
        public final ActorRef sender;
        public final Integer key;
        public ReadRequestMsg(ActorRef sender, Integer key) {
            this.sender = sender;
            this.key = key;
        }
    }

    public static class ReadResponseMsg implements Serializable {
        public final ActorRef recipient;
        public final String value;
        public ReadResponseMsg(ActorRef recipient, String value) {
            this.recipient = recipient;
            this.value = value;
         }
    }
}
