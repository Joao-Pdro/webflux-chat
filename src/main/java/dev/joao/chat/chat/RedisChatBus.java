package dev.joao.chat.chat;

import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

@Component
@Profile("redis")
public class RedisChatBus implements ChatBus, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(RedisChatBus.class);

    // decremento + remoção atômicos: sem isso, um join entre o HINCRBY e o HDEL seria apagado
    private static final RedisScript<Long> LEAVE = RedisScript.of("""
            local n = redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
            if n <= 0 then redis.call('HDEL', KEYS[1], ARGV[1]) return 1 end
            return 0
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final ReactiveRedisMessageListenerContainer listener; // uma conexão de pub/sub compartilhada
    private final JsonMapper json;

    public RedisChatBus(ReactiveStringRedisTemplate redis, ReactiveRedisConnectionFactory factory, JsonMapper json) {
        this.redis = redis;
        this.listener = new ReactiveRedisMessageListenerContainer(factory);
        this.json = json;
    }

    private static String channel(String room) {
        return "chat:" + room;
    }

    private static String presenceKey(String room) {
        return "presence:" + room;
    }

    @Override
    public Mono<Void> publish(String room, ChatEvent event) {
        return Mono.fromCallable(() -> json.writeValueAsString(event))
                .flatMap(payload -> redis.convertAndSend(channel(room), payload))
                .then();
    }

    @Override
    public Flux<ChatEvent> receive(String room) {
        return listener.receive(ChannelTopic.of(channel(room)))
                .<ChatEvent>handle((m, out) -> {
                    try {
                        out.next(json.readValue(m.getMessage(), ChatEvent.class));
                    } catch (RuntimeException e) {
                        log.warn("evento inválido descartado na sala {}: {}", room, e.getMessage());
                    }
                });
    }

    // ponytail: se uma instância cair sem encerrar as conexões, os contadores ficam presos; fix = chave por instância com TTL e heartbeat
    @Override
    public Mono<Boolean> join(String room, String user) {
        return redis.opsForHash().increment(presenceKey(room), user, 1).map(n -> n == 1);
    }

    @Override
    public Mono<Boolean> leave(String room, String user) {
        return redis.execute(LEAVE, List.of(presenceKey(room)), List.of(user)).next().map(n -> n == 1L);
    }

    @Override
    public Mono<Collection<String>> online(String room) {
        return redis.opsForHash().keys(presenceKey(room)).map(String::valueOf).collectList().map(l -> l);
    }

    @Override
    public void destroy() {
        listener.destroy();
    }
}
