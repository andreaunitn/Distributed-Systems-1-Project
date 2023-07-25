package disi.unitn.michele.andrea;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import disi.unitn.michele.andrea.Client;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        int N_CLIENTS = 2;

        final ActorSystem system = ActorSystem.create("DHT");
        List<ActorRef> clients = new ArrayList<>();

        for(int i = 0; i < N_CLIENTS; i++) {
            clients.add(system.actorOf(Client.props(), "Client " + (i+1)));
        }

        system.terminate();
    }
}