package disi.unitn.michele.andrea;

import akka.actor.Actor;
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
            System.out.println("\t 6: Print network;");
            System.out.println("\t 7: Exit;");
            System.out.print("Operation: ");

            int op = in.nextInt();

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

                    System.out.println("\t Client-s successfully created\n");

                    break;

                case 2:
                    System.out.print("\t How many nodes to create? ");
                    int N_NODES = in.nextInt();

                    while(N_NODES < 1 || N_NODES > 1024) {
                        System.out.print("\t How many nodes to create? ");
                        N_NODES = in.nextInt();
                    }

                    for(int i = 0; i < N_NODES; i++) {
                        System.out.print("\t\t Key (0-1023): ");
                        Integer k = in.nextInt();

                        while(k < 0 || k > 1023) {
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

                        if(!ring.HashTable.isEmpty()) {

                            Random generator = new Random();
                            Object[] values = ring.AvailableNodes.toArray();
                            ActorRef randomBootstrapper = (ActorRef) values[generator.nextInt(values.length)];

                            // TODO: check if the selected bootstrapped node is available

                            node.tell(new Message.JoinNetworkOrder(randomBootstrapper), ActorRef.noSender());
                        }

                        ring.HashTable.put(k, node);
                        ring.AvailableNodes.add(node);
                    }

                    System.out.println("\t\t Node-s successfully created\n");

                    break;

                case 3:
                    if(ring.HashTable.size() == 0) {
                        System.out.println("\t Cannot delete any node because the network is empty");
                        System.out.println();
                        break;
                    }

                    System.out.print("\t Key: ");
                    Integer k = in.nextInt();

                    while(!ring.HashTable.containsKey(k)) {
                        System.out.println("\t No node to delete with the specified key: ");
                        System.out.print("\t Key: ");
                        k = in.nextInt();
                    }

                    ActorRef node = ring.HashTable.get(k);
                    node.tell(new Message.LeaveNetworkOrder(), ActorRef.noSender());
                    node.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());

                    ring.HashTable.remove(k);

                    System.out.println("\t\t Node successfully deleted\n");

                    break;

                case 4:
                    if(clients.size() < 1 || ring.HashTable.size() == 0) {
                        System.out.println("\t Cannot perform get because there are no clients or the network does not contain any node");
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

                    Random rand = new Random();
                    List<Integer> keysAsArray = new ArrayList<>(ring.HashTable.keySet());
                    ActorRef n = ring.HashTable.get(keysAsArray.get(rand.nextInt(keysAsArray.size())));

                    ActorRef c = clients.get(ClientKey);
                    c.tell(new Message.GetRequestOrderMsg(n, key), ActorRef.noSender());

                    TimeUnit.MILLISECONDS.sleep(500);
                    System.out.println();

                    break;

                case 5:
                    break;

                case 6:
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

                case 7:
                    exit = true;
                    break;

                default: System.out.println("Operation not supported");
            }
        }

        System.out.println("Terminating...");
        system.terminate();
    }
}