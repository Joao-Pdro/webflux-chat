package dev.joao.chat.chat;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Component
@Profile("!redis")
public class LocalChatBus implements ChatBus {

    private record RoomEvent(String room, ChatEvent event) {
    }

    // ponytail: um sink global filtrado por sala custa O(assinantes) por evento; sink por sala se isso pesar
    private final Sinks.Many<RoomEvent> events = Sinks.many().multicast().directBestEffort();
    // sala -> usuário -> nº de conexões; a sala sai do mapa quando fica vazia
    private final Map<String, Map<String, Integer>> presence = new ConcurrentHashMap<>();

    @Override
    public Mono<Void> publish(String room, ChatEvent event) {
        return Mono.fromRunnable(() ->
                events.emitNext(new RoomEvent(room, event), Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(100))));
    }

    @Override
    public Flux<ChatEvent> receive(String room) {
        return events.asFlux().filter(e -> e.room().equals(room)).map(RoomEvent::event);
    }

    @Override
    public Mono<Boolean> join(String room, String user) {
        return Mono.fromSupplier(() -> {
            boolean[] first = {false};
            presence.compute(room, (k, users) -> {
                Map<String, Integer> m = users != null ? users : new ConcurrentHashMap<>();
                first[0] = m.merge(user, 1, Integer::sum) == 1;
                return m;
            });
            return first[0];
        });
    }

    @Override
    public Mono<Boolean> leave(String room, String user) {
        return Mono.fromSupplier(() -> {
            boolean[] last = {false};
            presence.computeIfPresent(room, (k, users) -> {
                last[0] = users.compute(user, (u, n) -> n == null || n <= 1 ? null : n - 1) == null;
                return users.isEmpty() ? null : users;
            });
            return last[0];
        });
    }

    @Override
    public Mono<Collection<String>> online(String room) {
        return Mono.fromSupplier(() -> List.copyOf(presence.getOrDefault(room, Map.of()).keySet()));
    }
}
