package disi.unitn.michele.andrea;

import scala.concurrent.duration.Duration;
import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.concurrent.TimeUnit;
import java.util.HashSet;

public class Client extends AbstractActor {

    private final int key;
    private final HashSet<Integer> write_requests;
    private final HashSet<Integer> read_requests;
    private final int T; // timeout in ms
    private int counter = 0; // counter to be used for message ids. Gets increased at every use


    /***** Constructor *****/
    public Client(int key, int T) {
        this.key = key;
        this.write_requests = new HashSet<>();
        this.read_requests = new HashSet<>();
        this.T = T;
    }

    static public Props props(int key, int T) {
        return Props.create(Client.class, () -> new Client(key, T));
    }


    /*@Override
    public Receive createReceive() {
        return receiveBuilder()
                // Join
                .match(MessageClient.JoinSystemMsg.class, this::OnJoinSystem)

                // Requests
                .match(Message.GetRequestOrderMsg.class, this::OnGetRequestOrder)
                .match(Message.UpdateRequestOrderMsg.class, this::OnUpdateRequestOrder)

                // Responses
                .match(Message.ReadResponseMsg.class, this::OnReadResponse)
                .match(Message.WriteResponseMsg.class, this::OnWriteResponse)

                // Timeout
                .match(Message.TimeoutMsg.class, this::OnTimeOut)

                // Log
                .match(Message.PrintClient.class, this::OnPrintClient)
                .match(Message.ErrorMsg.class, this::OnError)

                .build();
    }*/


    /***** Dispatcher *****/
    @Override
    public Receive createReceive() {
        return receiveBuilder()

                // Join
                .match(MessageClient.JoinSystemMsg.class, this::OnJoinSystem)

                // Requests
                .match(MessageClient.GetRequestMsg.class, this::OnGetRequest)
                .match(MessageClient.UpdateRequestMsg.class, this::OnUpdateRequest)

                // Responses
                .match(MessageClient.GetResponseMsg.class, this::OnGetResponse)
                .match(MessageClient.UpdateResponseMsg.class, this::OnUpdateResponse)

                // Timeout
                .match(MessageClient.GetTimeoutMsg.class, this::OnGetTimeout)
                .match(MessageClient.UpdateTimeoutMsg.class,this::OnUpdateTimeout)

                // Log
                .match(MessageClient.PrintSelfMsg.class, this::OnPrintSelf)
                .match(MessageClient.PrintErrorMsg.class, this::OnPrintError)

                .build();
    }

    /***** Actor methods *****/
    ////////////////////
    // Join
    private void OnJoinSystem(MessageClient.JoinSystemMsg m) {
        log("joined the system");
    }

    ////////////////////
    // Requests
    private void OnGetRequest(MessageClient.GetRequestMsg m) {

        // Creating new read request for the coordinator node
        MessageNode.GetRequestMsg req = new MessageNode.GetRequestMsg(m.key, counter);
        this.read_requests.add(counter);

        // Timeout
        SetTimeout(new MessageClient.GetTimeoutMsg(getSelf(), m.key, "Read timeout", counter));
        m.node_coordinator.tell(req, getSelf());

        counter += 1;
    }

    private void OnUpdateRequest(MessageClient.UpdateRequestMsg m) {

        // Creating a new write request for the coordinator node
        MessageNode.UpdateRequestMsg req = new MessageNode.UpdateRequestMsg(m.key, m.value, counter); //ex write
        this.write_requests.add(counter);

        // Timeout
        SetTimeout(new MessageClient.UpdateTimeoutMsg(getSelf(), m.key, "Write timeout", counter));
        m.node_coordinator.tell(req, getSelf());

        counter += 1;
    }

    ////////////////////
    // Responses

    private void OnGetResponse(MessageClient.GetResponseMsg m) {
        log("received " + "(" + m.key + ", " + m.entry.GetValue() + ", ver: " + m.entry.GetVersion() + ")");

        // Remove read request because is completed
        this.read_requests.remove(m.msg_id);
    }

    private void OnUpdateResponse(MessageClient.UpdateResponseMsg m) {
        log(m.value + " written");

        // Remove write request because is completed
        this.write_requests.remove(m.msg_id);
    }

    ////////////////////
    // Timeouts

    private void OnGetTimeout(MessageClient.GetTimeoutMsg m) {

        // Read request not completed
        if(this.read_requests.contains(m.msg_id)) {
            getSelf().tell(new MessageClient.PrintErrorMsg("cannot read value for key: " + m.key), getSelf());
            this.read_requests.remove(m.msg_id);
        }
    }

    private void OnUpdateTimeout(MessageClient.UpdateTimeoutMsg m) {

        // Write request not completed
        if (this.write_requests.contains(m.msg_id)) {
            getSelf().tell(new MessageClient.PrintErrorMsg("cannot write value for key: " + m.key), getSelf());
            this.write_requests.remove(m.msg_id);
        }
    }

    ////////////////////
    // Log
    private void OnPrintSelf(MessageClient.PrintSelfMsg m) {
        log("");
    }

    private void OnPrintError(MessageClient.PrintErrorMsg m) {
        log_error(m.msg);
    }

    /***** Additional functions *****/
    private void log(String m) {
        System.out.println("[Client " + this.key + "] " + m);
    }

    private void log_error(String m) {
        System.err.println("[Client " + this.key + "] " + m);
    }

    private void SetTimeout(MessageClient.BaseTimeout m) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(this.T, TimeUnit.MILLISECONDS), // how frequently generate them
                getSelf(),                                       // destination actor reference
                m,                                               // the message to send
                getContext().system().dispatcher(),              // system dispatcher
                getSelf()                                        // source of the message (myself)
        );
    }

    /**********/















    // Client receives a get request from main
    private void OnGetRequestOrder(Message.GetRequestOrderMsg m) {

        Message.ReadRequestMsg req = new Message.ReadRequestMsg(getSelf(), m.key);
        this.read_requests.add(req.message_id);

        // Timeout
        SetTimeout(new Message.TimeoutMsg(getSelf(), m.key, "Read time-out", req.message_id, "read"));
        m.node.tell(req, getSelf());
    }

    // When receives the response for a read print data item
    private void OnReadResponse(Message.ReadResponseMsg m) {
        System.out.println("\t\t Client " + this.key + " received value " + m.value.GetValue() + " for key " + m.key);
        this.read_requests.remove(m.message_id);
    }

    // Receives a write response message from the coordinator
    /*private void OnWriteResponse(Message.WriteResponseMsg m) {

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
    }*/

    // Client receives an update request from main
    private void OnUpdateRequestOrder(Message.UpdateRequestOrderMsg m) {

        Message.WriteRequestMsg req = new Message.WriteRequestMsg(getSelf(), m.key, m.value);
        this.write_requests.add(req.message_id);

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

            if (this.write_requests.contains(m.message_id)) {
                getSelf().tell(new Message.ErrorMsg("Cannot update value for key: " + m.key), getSelf());
                this.write_requests.remove(m.message_id);
            }

        } else if ("read".equals(m.operation)) {

            // if read request han not been satisfied, error is thrown
            if(this.read_requests.contains(m.message_id)) {
                getSelf().tell(new Message.ErrorMsg("Cannot read value for key: " + m.key), getSelf());
                this.read_requests.remove(m.message_id);
            }
        }
    }

    private void SetTimeout(Message.BaseMessage m) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(T, TimeUnit.MILLISECONDS), // how frequently generate them
                getSelf(),                                       // destination actor reference
                m,                                               // the message to send
                getContext().system().dispatcher(),              // system dispatcher
                getSelf()                                        // source of the message (myself)
        );
    }
}
