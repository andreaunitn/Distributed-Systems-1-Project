package disi.unitn.michele.andrea;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import disi.unitn.michele.andrea.Client;
import disi.unitn.michele.andrea.Node;
import disi.unitn.michele.andrea.DHT;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.List;


public class Main {
    public static void main(String[] args) {

        final ActorSystem system = ActorSystem.create("DHT");
        List<ActorRef> clients = new ArrayList<>();

        DHT ring = new DHT();
        int client_id = 0;

        Scanner in = new Scanner(System.in);

        System.out.println("Welcome to our Distributed Systems 1 Project!");
        System.out.println("Network Initialized");

        boolean exit = false;
        while(!exit) {
            System.out.println("Choose the operation you want to perform:");
            System.out.println("\t 1: Create clients;");
            System.out.println("\t 2: Create nodes;");
            System.out.println("\t 3: Print network;");
            System.out.println("\t 4: Exit;");
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
                        clients.add(system.actorOf(Client.props(), Integer.toString(client_id)));
                        client_id++;
                    }

                    System.out.println("\t Clients successfully created\n");

                    break;

                case 2:
                    System.out.print("\t How many nodes to create? ");
                    int N_NODES = in.nextInt();

                    while(N_NODES < 1) {
                        System.out.print("\t How many nodes to create? ");
                        N_NODES = in.nextInt();
                    }

                    for(int i = 0; i < N_NODES; i++) {
                        System.out.print("\t\t Key: ");
                        Integer k = in.nextInt();

                        while(k < 0) {
                            System.out.println("\t\t Wrong key");
                            System.out.print("\t\t Key: ");
                            k = in.nextInt();
                        }

                        while(ring.HashTable.containsKey(k)) {
                            System.out.println("\t\t The specified key already exists, please choose another one");
                            System.out.print("\t\t Key: ");
                            k = in.nextInt();
                        }

                        ring.HashTable.put(k, system.actorOf(Node.props(k), Integer.toString(k)));
                    }

                    break;

                case 3:
                    ring.PrintNetwork();
                    break;

                case 4:
                    exit = true;
                    break;

                default: System.out.println("Operation not supported");
            }
        }

        System.out.println("Terminating...");
        system.terminate();
    }
}