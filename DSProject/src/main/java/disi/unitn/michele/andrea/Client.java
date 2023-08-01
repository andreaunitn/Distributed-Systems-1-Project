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

    // Dispatcher
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.GetRequestOrderMsg.class, this::OnGetRequestOrder)
                .match(Message.PrintClient.class, this::OnPrintClient)
                .match(Message.ReadResponseMsg.class, this::OnReadResponse)
                .match(Message.ErrorMsg.class, this::OnError)
                .build();
    }

    // Client receives a get request from main
    private void OnGetRequestOrder(Message.GetRequestOrderMsg m) {
        m.node.tell(new Message.ReadRequestMsg(getSelf(), m.key), getSelf());
    }

    // When receives the response for a read print data item
    private void OnReadResponse(Message.ReadResponseMsg m) {
        System.out.println("\t\t Client " + this.key + " received value " + m.value.GetValue() + " for key " + m.key);
    }

    // Print errors
    private void OnError(Message.ErrorMsg m) {
        System.err.println(m.msg);
    }

    // Print client
    private void OnPrintClient(Message.PrintClient m) {
        System.out.println("\t Client " + this.key);
    }
}
