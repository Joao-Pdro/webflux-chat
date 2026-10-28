package dev.joao.chat.chat;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

@Service
public class ChatService {

    private static final Logger log = LoggerFactory.getLogger(ChatService.class);

    /** Cliente lento: guarda as 100 mais novas por assinante e descarta as mais antigas. */
    private static final int SUBSCRIBER_BUFFER = 100;

    /** Estado local desta instância para uma sala: fan-out para os clientes conectados aqui. */
    private static final class Room {
        // directBestEffort: o sink sobrevive quando o último assinante sai (onBackpressureBuffer encerraria a sala)
        final Sinks.Many<ChatEvent> sink = Sinks.many().multicast().directBestEffort();
        int connections;  // conexões abertas nesta instância; só muda dentro de rooms.compute
        Disposable feed;  // bus -> sink
    }

    // Uma sala existe aqui enquanto houver alguma conexão local; sai do mapa (e desinscreve do bus) com a última.
    private final Map<String, Room> rooms = new ConcurrentHashMap<>();
    private final MessageRepository repo;
    private final ChatBus bus;

    public ChatService(MessageRepository repo, ChatBus bus) {
        this.repo = repo;
        this.bus = bus;
    }

    // grava primeiro e publica o objeto devolvido pelo save (é o que tem o id)
    public Mono<Message> send(String room, String user, String text) {
        return repo.save(new Message(null, room, user, text, Instant.now()))
                .flatMap(saved -> publish(room, ChatEvent.message(saved)).thenReturn(saved));
    }

    /** Evento efêmero: não vai para o banco nem para o histórico. */
    public Mono<Void> typing(String room, String user) {
        return publish(room, ChatEvent.typing(user));
    }

    public Flux<Message> history(String room) {
        return repo.findTop50ByRoomOrderByCreatedAtDesc(room)
                .collectList()
                .flatMapMany(list -> {
                    Collections.reverse(list);
                    return Flux.fromIterable(list);
                });
    }

    // Ordem: histórico, quem está online, ao vivo. A sala local é registrada antes de entrar na presença, para já
    // ouvir os eventos dos outros, e tudo é desfeito no doFinally (cancelamento do cliente, erro ou fim).
    // ponytail: evento enviado entre o fim do histórico e a assinatura do live se perde para esse cliente; fix = id no SSE + Last-Event-ID
    public Flux<ChatEvent> stream(String room, String user) {
        return Flux.defer(() -> {
            Room r = acquire(room);
            boolean[] joined = {false};
            return bus.join(room, user)
                    .doOnNext(first -> joined[0] = true)
                    .flatMap(first -> first ? publish(room, ChatEvent.joined(user)) : Mono.empty())
                    .thenMany(history(room).map(ChatEvent::message)
                            .concatWith(bus.online(room).map(ChatEvent::presence))
                            .concatWith(r.sink.asFlux()
                                    // o "joined" do próprio usuário já está no snapshot de presença; pelo Redis ele ainda
                                    // voltaria (às vezes) depois dele, e a ordem não pode depender dessa corrida
                                    .filter(e -> !(e.type().equals("joined") && e.isAbout(user)))
                                    .onBackpressureBuffer(SUBSCRIBER_BUFFER, BufferOverflowStrategy.DROP_OLDEST)))
                    .doFinally(signal -> {
                        release(room);
                        if (joined[0]) leave(room, user);
                    });
        });
    }

    int activeRooms() {
        return rooms.size();
    }

    private Room acquire(String room) {
        return rooms.compute(room, (k, existing) -> {
            Room r = existing;
            if (r == null) {
                Room created = r = new Room();
                created.feed = bus.receive(room)
                        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(30))
                                .doBeforeRetry(s -> log.warn("bus da sala {} caiu, reconectando: {}", room, s.failure().getMessage())))
                        .subscribe(e -> created.sink.emitNext(e, Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(100))));
            }
            r.connections++;
            return r;
        });
    }

    private void release(String room) {
        Disposable[] feed = {null};
        rooms.computeIfPresent(room, (k, r) -> {
            if (--r.connections > 0) return r;
            feed[0] = r.feed;
            return null;
        });
        if (feed[0] != null) feed[0].dispose();
    }

    private void leave(String room, String user) {
        bus.leave(room, user)
                .flatMap(last -> last ? publish(room, ChatEvent.left(user)) : Mono.empty())
                .subscribe(v -> { }, e -> log.warn("falha ao sair da sala {}: {}", room, e.getMessage()));
    }

    // Se o bus falhar, a mensagem já está no banco (aparece no histórico); só a entrega ao vivo é perdida.
    private Mono<Void> publish(String room, ChatEvent event) {
        return bus.publish(room, event).onErrorResume(e -> {
            log.warn("falha ao publicar {} na sala {}: {}", event.type(), room, e.getMessage());
            return Mono.empty();
        });
    }
}
