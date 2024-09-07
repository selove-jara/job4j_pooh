package ru.job4j.pooh;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PoohServer {
    private final QueueSchema queueSchema = new QueueSchema();
    private final TopicSchema topicSchema = new TopicSchema();

    private void runSchemas() {
        ExecutorService pool = Executors.newCachedThreadPool();
        pool.execute(queueSchema);
        pool.execute(topicSchema);
    }

    private void runServer() {
        ExecutorService pool = Executors.newCachedThreadPool();
        try (ServerSocket server = new ServerSocket(9000)) {
            System.out.println("Pooh is ready ...");
            while (!server.isClosed()) {
                Socket socket = server.accept();
                pool.execute(() -> {
                    try (OutputStream out = socket.getOutputStream();
                         var input = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                        processClient(input, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processClient(BufferedReader input, OutputStream out) throws IOException {
        String line;
        while ((line = input.readLine()) != null) {
            var details = line.split(";");
            if (details.length != 3) {
                continue;
            }
            var action = details[0];
            var name = details[1];
            var text = details[2];

            if (action.equals("intro")) {
                if (name.equals("queue")) {
                    queueSchema.addReceiver(new SocketReceiver(text, new PrintWriter(out)));
                } else if (name.equals("topic")) {
                    topicSchema.addReceiver(new SocketReceiver(text, new PrintWriter(out)));
                }
            } else if (action.equals("queue")) {
                queueSchema.publish(new Message(name, text));
            } else if (action.equals("topic")) {
                topicSchema.publish(new Message(name, text));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        var pooh = new PoohServer();
        pooh.runSchemas();
        pooh.runServer();
    }
}