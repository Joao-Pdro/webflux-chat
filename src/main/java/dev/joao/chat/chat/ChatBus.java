package dev.joao.chat.chat;

import java.util.Collection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Por onde os eventos das salas e a presença circulam entre instâncias.
 * {@link LocalChatBus}: memória, uma instância. {@link RedisChatBus}: Redis, várias instâncias.
 */
public interface ChatBus {

    Mono<Void> publish(String room, ChatEvent event);

    /** Eventos publicados na sala por qualquer instância, a partir da assinatura. */
    Flux<ChatEvent> receive(String room);

    /** Emite {@code true} se esta é a primeira conexão do usuário na sala, somando todas as instâncias. */
    Mono<Boolean> join(String room, String user);

    /** Emite {@code true} se esta era a última conexão do usuário na sala, somando todas as instâncias. */
    Mono<Boolean> leave(String room, String user);

    Mono<Collection<String>> online(String room);
}
