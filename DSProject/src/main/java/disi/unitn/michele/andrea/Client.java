package disi.unitn.michele.andrea;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Client extends AbstractActor {

    private final Integer key;

    public Client(Integer key) {
        this.key = key;
    }

    static public Props props(Integer key) {
        return Props.create(Client.class, () -> new Client(key));
    }

    // TODO 4: To be adapted for our code
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.PrintClient.class, this::OnPrintClient)
                .build();
    }

    private void OnPrintClient(Message.PrintClient m) {
        System.out.println("\t Client " + this.key);
    }
}
