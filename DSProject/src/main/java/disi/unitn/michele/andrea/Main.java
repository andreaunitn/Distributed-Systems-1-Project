package disi.unitn.michele.andrea;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;

import java.util.concurrent.TimeUnit;
import java.util.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        final ActorSystem system = ActorSystem.create("DHT");
        HashMap<Integer, ActorRef> clients = new HashMap<>();

        DHT ring = new DHT();
        int client_id = 0;
        final int MAX_NODES = 1024;

        Scanner in = new Scanner(System.in);
        Random rand = new Random();

        System.out.println("Welcome to our Distributed Systems 1 Project!");

        //int N=3, R=2, W=2;
        int N=1, R=1, W=11;
        int T=200;
        
        System.out.println("Network Initialized");

        boolean exit = false;
        while(!exit) {
            System.out.println("Choose the operation you want to perform:");
            System.out.println("\t 1: Create clients;");
            System.out.println("\t 2: Create nodes;");
            System.out.println("\t 3: Delete node;");
            System.out.println("\t 4: Get;");
            System.out.println("\t 5: Update;");
            System.out.println("\t 6: Crash node;");
            System.out.println("\t 7: Recover node;");
            System.out.println("\t 8: Print network;");
            System.out.println("\t 9: Exit;");
            System.out.print("Operation: ");

            int op = in.nextInt();

            // All possible cases
            switch(op) {
                case 1:
                    System.out.print("\t How many clients to create? ");
                    int num_clients = in.nextInt();

                    while(num_clients < 1) {
                        System.out.print("\t How many clients to create? ");
                        num_clients = in.nextInt();
                    }

                    for(int i = 0; i < num_clients; i++) {
                        clients.put(client_id, system.actorOf(Client.props(client_id, T), "Client" + client_id));
                        clients.get(client_id).tell(new MessageClient.JoinSystemMsg(), ActorRef.noSender());
                        client_id++;
                    }

                    break;

                case 2:
                    System.out.print("\t How many nodes to create? ");
                    int num_nodes = in.nextInt();

                    while(num_nodes < 1 || num_nodes > MAX_NODES) {
                        System.out.print("\t How many nodes to create? ");
                        num_nodes = in.nextInt();
                    }

                    for(int i = 0; i < num_nodes; i++) {
                        System.out.print("\t\t Key (0-1023): ");
                        int k = in.nextInt();

                        while(k < 0 || k > (MAX_NODES - 1)) {
                            System.out.println("\t\t Wrong key");
                            System.out.print("\t\t Key (0-1023): ");
                            k = in.nextInt();
                        }

                        while(ring.hash_table.containsKey(k)) {
                            System.out.println("\t\t The specified key already exists, please choose another one");
                            System.out.print("\t\t Key (0-1023): ");
                            k = in.nextInt();
                        }

                        ActorRef node = system.actorOf(Node.props(k, N, R, W, T), "Node" + k);

                        if(!ring.available_nodes.isEmpty()) {

                            Random generator = new Random();
                            Object[] values = ring.available_nodes.toArray();
                            ActorRef randomBootstrapper = (ActorRef) values[generator.nextInt(values.length)];
                            node.tell(new MessageNode.JoinSystemMsg(randomBootstrapper), ActorRef.noSender());
                        }

                        ring.hash_table.put(k, node);
                        ring.available_nodes.add(node);
                    }

                    // TODO: stampa nel nodo
                    System.out.println("\t\t Node successfully created\n");
                    break;

                case 3:
                    if(ring.hash_table.isEmpty() || ring.available_nodes.size() < 2) {
                        System.out.println("\t Cannot delete any node because the network is empty or there's only one node available");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Key: ");
                    int k = in.nextInt();

                    while(!ring.hash_table.containsKey(k) && !ring.available_nodes.contains(ring.hash_table.get(k))) {
                        System.out.println("\t No node to delete with the specified key (might be in crashed state)");
                        System.out.print("\t Key: ");
                        k = in.nextInt();
                    }

                    ActorRef node = ring.hash_table.get(k);
                    node.tell(new Message.LeaveNetworkOrder(), ActorRef.noSender());

                    ring.hash_table.remove(k);
                    ring.available_nodes.remove(node);

                    System.out.println("\t\t Node successfully deleted\n");
                    break;

                case 4:
                    if(clients.isEmpty() || ring.available_nodes.isEmpty()) {
                        System.out.println("\t Cannot perform get because there are no clients or the network does not contain any available node");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Select client: ");
                    int client_key = in.nextInt();

                    while(!clients.containsKey(client_key)) {
                        System.out.println("\t There is no client with the specified key");
                        System.out.print("\t Select client: ");
                        client_key = in.nextInt();
                    }

                    System.out.print("\t\t Key: ");
                    int key = in.nextInt();

                    List<Integer> keys_array = new ArrayList<>(ring.hash_table.keySet());
                    ActorRef n = ring.hash_table.get(keys_array.get(rand.nextInt(keys_array.size())));
                    ActorRef c = clients.get(client_key);
                    c.tell(new MessageClient.GetRequestMsg(n, key), ActorRef.noSender());

                    TimeUnit.MILLISECONDS.sleep(400);
                    System.out.println();
                    break;

                case 5:
                    if(clients.isEmpty() || ring.available_nodes.isEmpty()) {
                        System.out.println("\t Cannot perform update because there are no clients or the network does not contain any available node");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Select client: ");
                    int c_key = in.nextInt();

                    while(!clients.containsKey(c_key)) {
                        System.out.println("\t There is no client with the specified key");
                        System.out.print("\t Select client: ");
                        c_key = in.nextInt();
                    }

                    System.out.print("\t\t Key: ");
                    int Key = in.nextInt();
                    System.out.println();

                    in.nextLine();

                    System.out.print("\t\t Value: ");
                    String value = in.nextLine();

                    List<ActorRef> keys_Array = new ArrayList<>(ring.available_nodes);
                    ActorRef Node = keys_Array.get(rand.nextInt(keys_Array.size()));
                    ActorRef Client = clients.get(c_key);
                    Client.tell(new MessageClient.UpdateRequestMsg(Node, Key, value), ActorRef.noSender());

                    TimeUnit.MILLISECONDS.sleep(500);
                    System.out.println();
                    break;

                case 6:

                    if(ring.hash_table.isEmpty() || ring.available_nodes.isEmpty()) {
                        System.out.println("\t Cannot make crash any node because there are no nodes available");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Key: ");
                    int crash_key = in.nextInt();

                    if(!ring.hash_table.containsKey(crash_key) || !ring.available_nodes.contains(ring.hash_table.get(crash_key))) {
                        System.out.println("\t No node to make crash with the specified key (might be already in crashed state)");
                        System.out.println();
                        break;
                    }

                    ActorRef crash_node = ring.hash_table.get(crash_key);
                    crash_node.tell(new Message.CrashRequestOrder(), ActorRef.noSender());

                    ring.available_nodes.remove(crash_node);

                    System.out.println("\t\t Crash request sent\n");
                    break;

                case 7:

                    if(ring.hash_table.isEmpty() || ring.available_nodes.size() == ring.hash_table.size()) {
                        System.out.println("\t Cannot recover any node because no node is crashed");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Key: ");
                    int recover_key = in.nextInt();

                    if(!ring.hash_table.containsKey(recover_key) || ring.available_nodes.contains(ring.hash_table.get(recover_key))) {
                        System.out.println("\t No node to recover with the specified key (might be already recovered)");
                        System.out.println();
                        break;
                    }

                    ActorRef recover_node = ring.hash_table.get(recover_key);
                    ActorRef recovery_boostrap_node = null;

                    if(!ring.available_nodes.isEmpty()) {

                        // Random node to be associated to the node that needs to recover
                        Random generator = new Random();
                        Object[] values = ring.available_nodes.toArray();
                        recovery_boostrap_node = (ActorRef) values[generator.nextInt(values.length)];

                    }

                    recover_node.tell(new Message.RecoveryRequestOrder(recovery_boostrap_node), ActorRef.noSender());
                    ring.available_nodes.add(recover_node);

                    System.out.println("\t\t Recovery request sent\n");
                    break;

                case 8:
                    System.out.println("Clients:");

                    clients.forEach((i, client) -> {
                        client.tell(new MessageClient.PrintSelfMsg(), ActorRef.noSender());

                        try {
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    System.out.println();
                    System.out.println("Nodes:");
                    ring.PrintNetwork();

                    TimeUnit.SECONDS.sleep(1);
                    break;

                case 9:
                    exit = true;
                    break;

                default: System.out.println("Operation not supported");
            }
        }

        System.out.println("Terminating...");
        system.terminate();
    }
}