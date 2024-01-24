package disi.unitn.michele.andrea;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;

import java.util.*;
import java.util.concurrent.TimeUnit;

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
                    int N_CLIENTS = in.nextInt();

                    while(N_CLIENTS < 1) {
                        System.out.print("\t How many clients to create? ");
                        N_CLIENTS = in.nextInt();
                    }

                    for(int i = 0; i < N_CLIENTS; i++) {
                        clients.put(client_id, system.actorOf(Client.props(client_id), "Client" + client_id));
                        client_id++;
                    }

                    System.out.println("\t Client successfully created\n");

                    break;

                case 2:
                    System.out.print("\t How many nodes to create? ");
                    int N_NODES = in.nextInt();

                    while(N_NODES < 1 || N_NODES > MAX_NODES) {
                        System.out.print("\t How many nodes to create? ");
                        N_NODES = in.nextInt();
                    }

                    for(int i = 0; i < N_NODES; i++) {
                        System.out.print("\t\t Key (0-1023): ");
                        Integer k = in.nextInt();

                        while(k < 0 || k > (MAX_NODES - 1)) {
                            System.out.println("\t\t Wrong key");
                            System.out.print("\t\t Key (0-1023): ");
                            k = in.nextInt();
                        }

                        while(ring.HashTable.containsKey(k)) {
                            System.out.println("\t\t The specified key already exists, please choose another one");
                            System.out.print("\t\t Key (0-1023): ");
                            k = in.nextInt();
                        }

                        ActorRef node = system.actorOf(Node.props(k), "Node" + k);

                        if(!ring.AvailableNodes.isEmpty()) {

                            Random generator = new Random();
                            Object[] values = ring.AvailableNodes.toArray();
                            ActorRef randomBootstrapper = (ActorRef) values[generator.nextInt(values.length)];
                            System.out.println(randomBootstrapper);
                            node.tell(new Message.JoinNetworkOrder(randomBootstrapper), ActorRef.noSender());
                        }

                        ring.HashTable.put(k, node);
                        ring.AvailableNodes.add(node);
                    }

                    System.out.println("\t\t Node successfully created\n");

                    break;

                case 3:
                    if(ring.HashTable.size() == 0) {
                        System.out.println("\t Cannot delete any node because the network is empty");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Key: ");
                    Integer k = in.nextInt();

                    while(!ring.HashTable.containsKey(k) && !ring.AvailableNodes.contains(ring.HashTable.get(k))) {
                        System.out.println("\t No node to delete with the specified key (might be in crashed state)");
                        System.out.print("\t Key: ");
                        k = in.nextInt();
                    }

                    ActorRef node = ring.HashTable.get(k);
                    node.tell(new Message.LeaveNetworkOrder(), ActorRef.noSender());

                    // Tell sender node to terminate
                    node.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());

                    ring.HashTable.remove(k);
                    ring.AvailableNodes.remove(node);

                    System.out.println("\t\t Node successfully deleted\n");

                    break;

                case 4:
                    if(clients.size() < 1 || ring.AvailableNodes.size() == 0) {
                        System.out.println("\t Cannot perform get because there are no clients or the network does not contain any available node");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Select client: ");
                    Integer ClientKey = in.nextInt();

                    while(!clients.containsKey(ClientKey) ) {
                        System.out.println("\t There is no client with the specified key");
                        System.out.print("\t Select client: ");
                        ClientKey = in.nextInt();
                    }

                    System.out.print("\t\t Key: ");
                    Integer key = in.nextInt();

                    List<Integer> keysAsArray = new ArrayList<>(ring.HashTable.keySet());
                    ActorRef n = ring.HashTable.get(keysAsArray.get(rand.nextInt(keysAsArray.size())));
                    ActorRef c = clients.get(ClientKey);
                    c.tell(new Message.GetRequestOrderMsg(n, key), ActorRef.noSender());

                    TimeUnit.MILLISECONDS.sleep(500);
                    System.out.println();

                    break;

                case 5:
                    if(clients.size() < 1 || ring.AvailableNodes.size() == 0) {
                        System.out.println("\t Cannot perform update because there are no clients or the network does not contain any available node");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Select client: ");
                    Integer Clientkey = in.nextInt();

                    while(!clients.containsKey(Clientkey) ) {
                        System.out.println("\t There is no client with the specified key");
                        System.out.print("\t Select client: ");
                        Clientkey = in.nextInt();
                    }

                    System.out.print("\t\t Key: ");
                    Integer Key = in.nextInt();
                    System.out.println();

                    in.nextLine();

                    System.out.print("\t\t Value: ");
                    String value = in.nextLine();

                    //List<Integer> keysArray = new ArrayList<>(ring.HashTable.keySet());
                    List<ActorRef> keysArray = new ArrayList<>(ring.AvailableNodes);
                    ActorRef Node = keysArray.get(rand.nextInt(keysArray.size()));
                    ActorRef Client = clients.get(Clientkey);
                    Client.tell(new Message.UpdateRequestOrderMsg(Node, Key, value), ActorRef.noSender());

                    TimeUnit.MILLISECONDS.sleep(500);
                    System.out.println();

                    break;

                case 6:

                    if(ring.HashTable.size() == 0 || ring.AvailableNodes.isEmpty()) {
                        System.out.println("\t Cannot make crash any node because there are no nodes available");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Key: ");
                    Integer crashKey = in.nextInt();

                    if(!ring.HashTable.containsKey(crashKey) || !ring.AvailableNodes.contains(ring.HashTable.get(crashKey))) {
                        System.out.println("\t No node to make crash with the specified key (might be already in crashed state)");
                        System.out.println();
                        break;
                    }

                    ActorRef crashNode = ring.HashTable.get(crashKey);
                    crashNode.tell(new Message.CrashRequestOrder(), ActorRef.noSender());

                    ring.AvailableNodes.remove(crashNode);

                    System.out.println("\t\t Crash request sent\n");

                    break;

                case 7:

                    if(ring.HashTable.size() == 0 || ring.AvailableNodes.size() == ring.HashTable.size()) {
                        System.out.println("\t Cannot recover any node because no node is crashed");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Key: ");
                    Integer recoverKey = in.nextInt();

                    if(!ring.HashTable.containsKey(recoverKey) || ring.AvailableNodes.contains(ring.HashTable.get(recoverKey))) {
                        System.out.println("\t No node to recover with the specified key (might be already recovered)");
                        System.out.println();
                        break;
                    }

                    ActorRef recoverNode = ring.HashTable.get(recoverKey);
                    ActorRef recoveryBoostrapNode = null;

                    if(ring.AvailableNodes.size() > 0) {

                        // Random node to be associated to the node that needs to recover
                        Random generator = new Random();
                        Object[] values = ring.AvailableNodes.toArray();
                        recoveryBoostrapNode = (ActorRef) values[generator.nextInt(values.length)];

                    }

                    recoverNode.tell(new Message.RecoveryRequestOrder(recoveryBoostrapNode), ActorRef.noSender());

                    ring.AvailableNodes.add(recoverNode);

                    System.out.println("\t\t Recovery request sent\n");

                    break;

                case 8:
                    System.out.println("Clients:");

                    clients.forEach((i, client) -> {
                        client.tell(new Message.PrintClient(), ActorRef.noSender());

                        try {
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    System.out.println();

                    System.out.println("Nodes:");
                    ring.PrintNetwork();

                    TimeUnit.SECONDS.sleep(3);
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