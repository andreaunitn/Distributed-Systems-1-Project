package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;

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
        public final HashMap<Integer, ActorRef> network;
        public final ActorRef sender;
        public JoinResponseMsg(HashMap<Integer, ActorRef> network, ActorRef sender) {
            this.network = (HashMap<Integer, ActorRef>) Collections.unmodifiableMap(network);
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
        public final HashMap<Integer, String> storage;
        public DataResponseMsg(HashMap<Integer, String> storage) {
            this.storage = (HashMap<Integer, String>) Collections.unmodifiableMap(storage);
        }
    }

}
