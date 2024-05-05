package disi.unitn.michele.andrea;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.util.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        final ActorSystem system = ActorSystem.create("DHT");
        HashMap<Integer, ActorRef> clients = new HashMap<>();

        DHT ring = new DHT();
        int client_id = 0;
        final int MAX_NODES = 1024;

        Random rand = new Random();

        System.out.println("Welcome to our Distributed Systems 1 Project!");

        ////////////////////////////////////////
        // EDIT THESE PARAMETERS BEFORE RUNNING
        int N = 3, R = 2, W = 2;
        int T = 200;
        boolean input_from_file = true;
        ////////////////////////////////////////

        System.out.println("Network Initialized");

        try {
            Scanner in;
            if(input_from_file) {
                File file = new File("test.txt");
                in = new Scanner(file);
            } else {
                in = new Scanner(System.in);
            }

            boolean exit = false;
            while (!exit) {
                if(!input_from_file) {
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
                }

                String op = in.nextLine();
                if (op.startsWith("#")) {
                    continue;
                }

                if (op.startsWith("d")) {
                    int delay = Integer.parseInt(op.split(" ")[1]);

                    try {
                        Thread.sleep(delay);
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }

                    continue;
                }

                int operation = Integer.parseInt(op);

                // All possible cases
                switch (operation) {
                    case 1:
                        if(!input_from_file) System.out.print("How many clients to create? ");
                        int num_clients = Integer.parseInt(in.nextLine());

                        while (num_clients < 1) {
                            if(!input_from_file) System.out.print("How many clients to create? ");
                            num_clients = Integer.parseInt(in.nextLine());
                        }

                        for (int i = 0; i < num_clients; i++) {
                            clients.put(client_id, system.actorOf(Client.props(client_id, T), "Client" + client_id));
                            clients.get(client_id).tell(new MessageClient.JoinSystemMsg(), ActorRef.noSender());
                            client_id++;
                        }

                        break;

                    case 2:
                        if(!input_from_file) System.out.print("How many nodes to create? ");
                        int num_nodes = Integer.parseInt(in.nextLine());

                        while (num_nodes < 1 || num_nodes > MAX_NODES) {
                            if(!input_from_file) System.out.print("How many nodes to create? ");
                            num_nodes = Integer.parseInt(in.nextLine());
                        }

                        for (int i = 0; i < num_nodes; i++) {
                            if(!input_from_file) System.out.print("Key (0-1023): ");
                            int k = Integer.parseInt(in.nextLine());

                            while (k < 0 || k > (MAX_NODES - 1)) {
                                if(!input_from_file) System.out.println("Wrong key");
                                if(!input_from_file) System.out.print("Key (0-1023): ");
                                k = Integer.parseInt(in.nextLine());
                            }

                            while (ring.hash_table.containsKey(k)) {
                                if(!input_from_file) System.out.println("The specified key already exists, please choose another one");
                                if(!input_from_file) System.out.print("Key (0-1023): ");
                                k = Integer.parseInt(in.nextLine());
                            }

                            ActorRef node = system.actorOf(Node.props(k, N, R, W, T), "Node" + k);

                            if (!ring.available_nodes.isEmpty()) {

                                Random generator = new Random();
                                Object[] values = ring.available_nodes.toArray();
                                ActorRef randomBootstrapper = (ActorRef) values[generator.nextInt(values.length)];
                                node.tell(new MessageNode.JoinSystemMsg(randomBootstrapper), ActorRef.noSender());
                            }

                            ring.hash_table.put(k, node);
                            ring.available_nodes.add(node);
                        }

                        break;

                    case 3:
                        if (ring.hash_table.isEmpty() || ring.available_nodes.size() < 2) {
                            if(!input_from_file) System.out.println("Cannot delete any node because the network is empty or there's only one node available");
                            if(!input_from_file) System.out.println();
                            break;
                        }

                        if(!input_from_file) System.out.print("Key: ");
                        int k = Integer.parseInt(in.nextLine());

                        while (!ring.hash_table.containsKey(k) || !ring.available_nodes.contains(ring.hash_table.get(k))) {
                            if(!input_from_file) System.out.println("No node to delete with the specified key (might be in crashed state)");
                            if(!input_from_file) System.out.print("Key: ");
                            k = Integer.parseInt(in.nextLine());
                        }

                        ActorRef node = ring.hash_table.get(k);
                        node.tell(new MessageNode.LeaveNetworkMsg(), ActorRef.noSender());

                        ring.hash_table.remove(k);
                        ring.available_nodes.remove(node);

                        break;

                    case 4:
                        if (clients.isEmpty() || ring.available_nodes.isEmpty()) {
                            if(!input_from_file) System.out.println("Cannot perform get because there are no clients or the network does not contain any available node");
                            if(!input_from_file) System.out.println();
                            break;
                        }

                        if(!input_from_file) System.out.print("Select client: ");
                        int client_key = Integer.parseInt(in.nextLine());

                        while (!clients.containsKey(client_key)) {
                            if(!input_from_file) System.out.println("There is no client with the specified key");
                            if(!input_from_file) System.out.print("Select client: ");
                            client_key = Integer.parseInt(in.nextLine());
                        }

                        if(!input_from_file) System.out.print("Key: ");
                        int key = Integer.parseInt(in.nextLine());

                        List<Integer> keys_array = new ArrayList<>(ring.hash_table.keySet());
                        ActorRef n = ring.hash_table.get(keys_array.get(rand.nextInt(keys_array.size())));
                        ActorRef c = clients.get(client_key);
                        c.tell(new MessageClient.GetRequestMsg(n, key), ActorRef.noSender());

                        if(!input_from_file) TimeUnit.MILLISECONDS.sleep(400);
                        if(!input_from_file) System.out.println();
                        break;

                    case 5:
                        if (clients.isEmpty() || ring.available_nodes.isEmpty()) {
                            if(!input_from_file) System.out.println("Cannot perform update because there are no clients or the network does not contain any available node");
                            if(!input_from_file) System.out.println();
                            break;
                        }

                        if(!input_from_file) System.out.print("Select client: ");
                        int c_key = Integer.parseInt(in.nextLine());

                        while (!clients.containsKey(c_key)) {
                            if(!input_from_file) System.out.println("There is no client with the specified key");
                            if(!input_from_file) System.out.print("Select client: ");
                            c_key = Integer.parseInt(in.nextLine());
                        }

                        if(!input_from_file) System.out.print("Key: ");
                        int Key = Integer.parseInt(in.nextLine());
                        if(!input_from_file) System.out.println();

                        if(!input_from_file) System.out.print("Value: ");
                        String value = in.nextLine();

                        List<ActorRef> keys_Array = new ArrayList<>(ring.available_nodes);
                        ActorRef Node = keys_Array.get(rand.nextInt(keys_Array.size()));
                        ActorRef Client = clients.get(c_key);
                        Client.tell(new MessageClient.UpdateRequestMsg(Node, Key, value), ActorRef.noSender());

                        if(!input_from_file) TimeUnit.MILLISECONDS.sleep(500);
                        if(!input_from_file) System.out.println();
                        break;

                    case 6:

                        if (ring.hash_table.isEmpty() || ring.available_nodes.isEmpty()) {
                            if(!input_from_file) System.out.println("Cannot make crash any node because there are no nodes available");
                            if(!input_from_file) System.out.println();
                            break;
                        }

                        if(!input_from_file) System.out.print("Key: ");
                        int crash_key = Integer.parseInt(in.nextLine());

                        if (!ring.hash_table.containsKey(crash_key) || !ring.available_nodes.contains(ring.hash_table.get(crash_key))) {
                            if(!input_from_file) System.out.println("No node to make crash with the specified key (might be already in crashed state)");
                            if(!input_from_file) System.out.println();
                            break;
                        }

                        ActorRef crash_node = ring.hash_table.get(crash_key);
                        crash_node.tell(new MessageNode.CrashRequestMsg(), ActorRef.noSender());

                        ring.available_nodes.remove(crash_node);

                        if(!input_from_file) System.out.println("Crash request sent\n");
                        break;

                    case 7:

                        if (ring.hash_table.isEmpty() || ring.available_nodes.size() == ring.hash_table.size()) {
                            if(!input_from_file) System.out.println("Cannot recover any node because no node is crashed");
                            if(!input_from_file) System.out.println();
                            break;
                        }

                        if(!input_from_file) System.out.print("\t Key: ");
                        int recover_key = Integer.parseInt(in.nextLine());

                        if (!ring.hash_table.containsKey(recover_key) || ring.available_nodes.contains(ring.hash_table.get(recover_key))) {
                            if(!input_from_file) System.out.println("No node to recover with the specified key (might be already recovered)");
                            if(!input_from_file) System.out.println();
                            break;
                        }

                        ActorRef recover_node = ring.hash_table.get(recover_key);
                        ActorRef recovery_boostrap_node = null;

                        if (!ring.available_nodes.isEmpty()) {

                            // Random node to be associated to the node that needs to recover
                            Random generator = new Random();
                            Object[] values = ring.available_nodes.toArray();
                            recovery_boostrap_node = (ActorRef) values[generator.nextInt(values.length)];

                        }

                        recover_node.tell(new MessageNode.RecoveryRequestMsg(recovery_boostrap_node), ActorRef.noSender());
                        ring.available_nodes.add(recover_node);

                        if(!input_from_file) System.out.println("Recovery request sent\n");
                        break;

                    case 8:
                        if(!input_from_file) System.out.println("Clients:");

                        clients.forEach((i, client) -> {
                            client.tell(new MessageClient.PrintSelfMsg(), ActorRef.noSender());

                            try {
                                TimeUnit.MILLISECONDS.sleep(100);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });

                        if(!input_from_file) System.out.println();
                        if(!input_from_file) System.out.println("Nodes:");
                        ring.PrintNetwork();

                        TimeUnit.SECONDS.sleep(1);
                        break;

                    case 9:
                        exit = true;
                        break;

                    default:
                        System.out.println("Operation not supported");
                }
            }

            System.out.println("Terminating...");
            system.terminate();

            in.close();
        }
        catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
        }
    }
}