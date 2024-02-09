package disi.unitn.michele.andrea;

import scala.concurrent.duration.Duration;
import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.Iterator;

public class Client extends AbstractActor {

    private final int key;
    private final HashSet<Message.WriteRequestMsg> write_requests;
    private final HashSet<Message.ReadRequestMsg> read_requests;

    public Client(int key) {
        this.key = key;
        this.write_requests = new HashSet<>();
        this.read_requests = new HashSet<>();
    }

    static public Props props(int key) {
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
        this.read_requests.add(req);

        // Timeout
        SetTimeout(new Message.TimeoutMsg(getSelf(), m.key, "Read time-out", req.message_id, "read"));
        m.node.tell(new Message.ReadRequestMsg(getSelf(), m.key, 1), getSelf());
    }

    // When receives the response for a read print data item
    private void OnReadResponse(Message.ReadResponseMsg m) {
        System.out.println("\t\t Client " + this.key + " received value " + m.value.GetValue() + " for key " + m.key);
    }

    // Receives a write response message from the coordinator
    private void OnWriteResponse(Message.WriteResponseMsg m) {

        Message.WriteRequestMsg write_request = null;
        for(Message.WriteRequestMsg w : this.write_requests) {
            if(w.message_id == m.message_id) {
                write_request = w;
                break;
            }
        }

        if(write_request != null) {
            this.write_requests.remove(write_request);
        }

        System.out.println("\t\t Value " + m.value + " was written");
    }

    // Client receives an update request from main
    private void OnUpdateRequestOrder(Message.UpdateRequestOrderMsg m) {

        Message.WriteRequestMsg req = new Message.WriteRequestMsg(getSelf(), m.key, m.value);
        this.write_requests.add(req);

        // Timeout
        SetTimeout(new Message.TimeoutMsg(getSelf(), m.key, "Write time-out", req.message_id, "write"));
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

        if ("write".equals(m.operation)) {
            Iterator<Message.WriteRequestMsg> iterator = this.write_requests.iterator();

            while (iterator.hasNext()) {
                Message.WriteRequestMsg w = iterator.next();

                if (w.message_id == m.message_id) {
                    m.recipient.tell(new Message.ErrorMsg("Cannot update value for key: " + m.key), getSelf());
                    iterator.remove();
                    break;
                }
            }
        } else if ("read".equals(m.operation)) {
            Iterator<Message.ReadRequestMsg> iterator = this.read_requests.iterator();

            while (iterator.hasNext()) {
                Message.ReadRequestMsg r = iterator.next();

                if (r.message_id == m.message_id) {
                    m.recipient.tell(new Message.ErrorMsg("Cannot read value for key: " + m.key), getSelf());
                    iterator.remove();
                    break;
                }
            }
        }
    }

    private void SetTimeout(Message.BaseMessage m) {
        SetTimeout(m, 200);
    }

    private void SetTimeout(Message.BaseMessage m, int msTimer) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(msTimer, TimeUnit.MILLISECONDS),                                                       // how frequently generate them
                getSelf(),                                                                                             // destination actor reference
                m,                                                                                                     // the message to send
                getContext().system().dispatcher(),                                                                    // system dispatcher
                getSelf()                                                                                              // source of the message (myself)
        );
    }
}
