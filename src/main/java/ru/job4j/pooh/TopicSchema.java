package ru.job4j.pooh;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class TopicSchema implements Schema {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new LinkedBlockingQueue<>());
        data.get(message.name()).add(message.text());
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            boolean messagesProcessed = false;

            for (var topicKey : receivers.keySet()) {
                var messages = data.getOrDefault(topicKey, new LinkedBlockingQueue<>());
                String messageText = messages.poll();

                if (messageText != null) {
                    messagesProcessed = true;
                    var receiversForTopic = receivers.get(topicKey);
                    for (Receiver receiver : receiversForTopic) {
                        receiver.receive(messageText);
                    }
                }
            }

            if (!messagesProcessed) {
                condition.off();
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
