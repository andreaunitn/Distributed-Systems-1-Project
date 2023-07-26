package disi.unitn.michele.andrea;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Client extends AbstractActor {

    public Client() {}

    static public Props props() {
        return Props.create(Client.class, () -> new Client());
    }

    // TODO 4: To be adapted for our code
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .build();
    }
}
