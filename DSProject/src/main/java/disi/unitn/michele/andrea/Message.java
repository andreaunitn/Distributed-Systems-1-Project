package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.io.Serializable;

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
}
