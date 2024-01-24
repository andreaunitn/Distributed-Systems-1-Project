package disi.unitn.michele.andrea;

import akka.actor.AbstractActor;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractActor {

    private final Integer key;
    private final HashSet<Message.WriteRequestMsg> writeRequests;
    private final HashSet<Message.ReadRequestMsg> readRequests;

    public Client(Integer key) {
        this.key = key;
        this.writeRequests = new HashSet<>();
        this.readRequests = new HashSet<>();
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
                .match(Message.UpdateRequestOrderMsg.class, this::OnUpdateRequestOrder)
                .match(Message.ErrorMsg.class, this::OnError)
                .match(Message.WriteResponseMsg.class, this::OnWriteResponse)
                .match(Message.TimeoutMsg.class, this::OnTimeOut)
                .build();
    }

    // Client receives a get request from main
    private void OnGetRequestOrder(Message.GetRequestOrderMsg m) {

        Message.ReadRequestMsg req = new Message.ReadRequestMsg(getSelf(), m.key);
        this.readRequests.add(req);

        getContext().system().scheduler().scheduleOnce(
                Duration.create(200, TimeUnit.MILLISECONDS),                    // how frequently generate them
                getSelf(),                                                       // destination actor reference
                new Message.TimeoutMsg(getSelf(), m.key, "Read time-out", req.message_id, "read"),       // the message to send
                getContext().system().dispatcher(),                              // system dispatcher
                getSelf()                                                        // source of the message (myself)
        );
        
        m.node.tell(new Message.ReadRequestMsg(getSelf(), m.key, 1), getSelf());
    }

    // When receives the response for a read print data item
    private void OnReadResponse(Message.ReadResponseMsg m) {
        System.out.println("\t\t Client " + this.key + " received value " + m.value.GetValue() + " for key " + m.key);
    }

    // Receives a write response message from the coordinator
    private void OnWriteResponse(Message.WriteResponseMsg m) {

        Message.WriteRequestMsg writeRequest = null;
        //System.out.println("is writeRequests Empty: " + writeRequests.isEmpty());
        for(Message.WriteRequestMsg w : this.writeRequests) {
            //System.out.println("w.key: " + w.key + "; w.sender: " + w.sender + "; m.key: " + m.key + "; m.recipient: " + m.recipient);
            System.out.println("w.message_id: " + w.message_id + "; m.message_id: " + m.message_id);
            if(w.message_id == m.message_id) {
                writeRequest = w;
                break;
            }
        }

        if(writeRequest != null) {
            this.writeRequests.remove(writeRequest);
        }

        System.out.println("\t\t Value " + m.value + " was written");
    }

    // Client receives an update request from main
    private void OnUpdateRequestOrder(Message.UpdateRequestOrderMsg m) {

        Message.WriteRequestMsg req = new Message.WriteRequestMsg(getSelf(), m.key, m.value);
        this.writeRequests.add(req);

        getContext().system().scheduler().scheduleOnce(
                Duration.create(200, TimeUnit.MILLISECONDS),                    // how frequently generate them
                getSelf(),                                                       // destination actor reference
                new Message.TimeoutMsg(getSelf(), m.key, "Write time-out", req.message_id, "write"),       // the message to send
                getContext().system().dispatcher(),                              // system dispatcher
                getSelf()                                                        // source of the message (myself)
        );

        m.node.tell(req, getSelf());
    }

    // Print errors
    private void OnError(Message.ErrorMsg m) {
        System.err.println(m.msg);
    }

    // Print client
    private void OnPrintClient(Message.PrintClient m) {
        System.out.println("\t Client " + this.key);
    }

    private void OnTimeOut(Message.TimeoutMsg m) {
        if(m.operation.equals("write")) {
            Message.WriteRequestMsg writeRequest = null;
            for(Message.WriteRequestMsg w : this.writeRequests) {
                System.out.println("Timeout w.message_id: " + w.message_id + "; m.message_id: " + m.message_id);
                if(w.message_id == m.message_id) {
                    writeRequest = w;
                    break;
                }
            }

            if(writeRequest != null) {
                m.recipient.tell(new Message.ErrorMsg("Cannot update value for key: " + m.key), getSelf());
                this.writeRequests.remove(writeRequest);
                System.out.println(getSelf() + "Removed writeRequest in OnTimeOut");
            }
        } else if(m.operation.equals("read")) {
            Message.ReadRequestMsg readRequest = null;
            for(Message.ReadRequestMsg r: this.readRequests) {
                System.out.println("Timeout w.message_id: " + r.message_id + "; m.message_id: " + m.message_id);
                if(r.message_id == m.message_id) {
                    readRequest = r;
                    break;
                }
            }

            if(readRequest != null) {
                m.recipient.tell(new Message.ErrorMsg("Cannot read value for key: " + m.key), getSelf());
                this.readRequests.remove(readRequest);
                System.out.println(getSelf() + "Removed readRequest in OnTimeOut");
            }
        }
    }
}
