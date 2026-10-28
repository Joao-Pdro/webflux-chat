package dev.joao.chat.chat;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Service
public class ChatService {

    private final Map<String, Sinks.Many<Message>> rooms = new ConcurrentHashMap<>();

    private Sinks.Many<Message> sink(String room) {
        // directBestEffort: o sink sobrevive quando o último assinante sai (onBackpressureBuffer encerraria a sala)
        return rooms.computeIfAbsent(room, r -> Sinks.many().multicast().directBestEffort());
    }

    public Message send(String room, String user, String text) {
        Message m = new Message(room, user, text, Instant.now());
        sink(room).emitNext(m, Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
        return m;
    }

    public Flux<Message> stream(String room) {
        return sink(room).asFlux();
    }
}
