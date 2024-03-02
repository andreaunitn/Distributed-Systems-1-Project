package disi.unitn.michele.andrea;

import java.io.Serializable;

/***** Messages received by a Node coming from the Main or Client *****/
public class MessageNode {

    ////////////////////
    // Requests
    public static class GetRequestMsg implements Serializable {
        public final int key;
        public final int msg_id;

        public GetRequestMsg(int key, int msg_id) {
            this.key = key;
            this.msg_id = msg_id;
        }
    }

    public static class UpdateRequestMsg implements Serializable {
        public final int key;
        public final String value;
        public final int msg_id;

        public UpdateRequestMsg(int key, String value, int msg_id) {
            this.key = key;
            this.value = value;
            this.msg_id = msg_id;
        }
    }

    ////////////////////
    // Utility
    public static class NetworkRequestMsg implements Serializable {}
}
