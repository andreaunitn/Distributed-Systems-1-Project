package disi.unitn.michele.andrea;

import akka.actor.Actor;
import akka.actor.ActorRef;
import scala.Int;

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

    public static class LeaveNetworkOrder implements Serializable {
        public LeaveNetworkOrder(){};
    }

    public static class CrashRequestOrder implements Serializable {
        public CrashRequestOrder(){}
    }

    public static class RecoveryRequestOrder implements Serializable {
        public final ActorRef node;
        public RecoveryRequestOrder(ActorRef node){
            this.node = node;
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
        public final Map<Integer, DataEntry> storage;
        public final Integer key;
        public DataResponseMsg(HashMap<Integer, DataEntry> storage, Integer key) {
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

    public static class NodeLeaveMsg implements Serializable {
        public final Integer key;
        public NodeLeaveMsg(Integer key) {
            this.key = key;
        }
    }

    public static class PassDataItemsMsg implements Serializable {
        public final Map<Integer, DataEntry> storage;
        public PassDataItemsMsg(HashMap<Integer, DataEntry> storage) {
            this.storage = Collections.unmodifiableMap(storage);
        }
    }

    public static class PrintNode implements Serializable {
        public PrintNode () {}
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
        public final Integer key;
        public final DataEntry value;
        public ReadResponseMsg(ActorRef recipient, Integer key, DataEntry value) {
            this.recipient = recipient;
            this.key = key;
            this.value = value;
         }
    }

    //TODO change name to message and relative functions
    public static class ErrorNoValueFound implements Serializable {
        public final ActorRef readSender;
        public final Integer key;
        public final DataEntry data;
        public ErrorNoValueFound(ActorRef readSender, Integer key, DataEntry data) {
            this.readSender = readSender;
            this.key = key;
            this.data = data;
        }
    }

    // Client
    public static class WriteRequestMsg implements Serializable {
        public final ActorRef sender;
        public final Integer key;
        public final String value;
        public WriteRequestMsg(ActorRef sender, Integer key, String value) {
            this.sender = sender;
            this.key = key;
            this.value = value;
        }
    }

    public static class WriteResponseMsg implements Serializable {
        public final String value;
        public WriteResponseMsg(String value){
            this.value = value;
        }
    }

    public static class WriteContentMsg implements Serializable {
        public final Integer key;
        public final DataEntry data;
        public WriteContentMsg(Integer key, DataEntry data) {
            this.key = key;
            this.data = data;
        }
    }

    public static class GetRequestOrderMsg implements Serializable {
        public final ActorRef node;
        public final Integer key;
        public GetRequestOrderMsg(ActorRef node, Integer key) {
            this.node = node;
            this.key = key;
        }
    }

    public static class UpdateRequestOrderMsg implements Serializable {
        public final ActorRef node;
        public final Integer key;
        public final String value;
        public UpdateRequestOrderMsg(ActorRef node, Integer key, String value) {
            this.node = node;
            this.key = key;
            this.value = value;
        }
    }

    public static class PrintClient implements Serializable {
        public PrintClient() {}
    }

    // Error handling
    public static class ErrorMsg implements Serializable {
        public final String msg;
        public ErrorMsg(String msg) {
            this.msg = msg;
        }
    }
}
