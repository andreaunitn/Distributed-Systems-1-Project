package disi.unitn.michele.andrea;

import akka.actor.ActorRef;

import java.io.Serializable;

public class Message {

    //Main
    public static class JoinNetwork implements Serializable {

        public final ActorRef bootstrapNode;

        public JoinNetwork(ActorRef node) {
            this.bootstrapNode = node;
        }

    }

    //Nodes
    public static class JoinRequestMsg implements Serializable {
        public final Integer id;
        public JoinRequestMsg(Integer key) {
            this.id = key;
        }
    }
}
