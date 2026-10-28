package dev.joao.chat.chat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Cada RedisChatBus/ChatService criado aqui faz o papel de uma instância da aplicação; o Redis é real. */
@Testcontainers
class RedisChatBusTest {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static LettuceConnectionFactory factory;
    static ReactiveStringRedisTemplate redis;
    static final JsonMapper JSON = JsonMapper.builder().build();

    final List<RedisChatBus> buses = new ArrayList<>();
    final List<Disposable> connections = new ArrayList<>();

    @BeforeAll
    static void connect() {
        factory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        factory.afterPropertiesSet();
        redis = new ReactiveStringRedisTemplate(factory);
    }

    @AfterAll
    static void disconnect() {
        factory.destroy();
    }

    @AfterEach
    void cleanUp() {
        connections.forEach(Disposable::dispose);
        buses.forEach(RedisChatBus::destroy);
    }

    private RedisChatBus newInstanceBus() {
        var bus = new RedisChatBus(redis, factory, JSON);
        buses.add(bus);
        return bus;
    }

    private ChatService newInstance() {
        var repo = mock(MessageRepository.class);
        when(repo.save(any())).thenAnswer(i -> Mono.just(i.getArgument(0)));
        when(repo.findTop50ByRoomOrderByCreatedAtDesc(any())).thenReturn(Flux.empty());
        return new ChatService(repo, newInstanceBus());
    }

    private static String room() {
        return "sala-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** O SUBSCRIBE é assíncrono: eventos publicados antes dele ser confirmado se perdem, então os testes esperam. */
    private static void awaitSubscribers(String room, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            String out = REDIS.execInContainer("redis-cli", "pubsub", "numsub", "chat:" + room).getStdout();
            if (out.trim().endsWith(String.valueOf(expected))) return;
            Thread.sleep(50);
        }
        throw new AssertionError("canal chat:" + room + " não chegou a " + expected + " assinante(s)");
    }

    private static boolean isMessage(ChatEvent e, String text) {
        return e.type().equals("message") && e.data() instanceof Map<?, ?> d && text.equals(d.get("text"));
    }

    private static boolean is(ChatEvent e, String type, String user) {
        return e.type().equals(type) && e.data() instanceof Map<?, ?> d && user.equals(d.get("user"));
    }

    // ---- RedisChatBus sozinho ----

    @Test
    void eventPublishedByOneInstanceIsReceivedByAnother() throws Exception {
        String room = room();
        var a = newInstanceBus();
        var b = newInstanceBus();
        var received = b.receive(room).take(1).collectList().toFuture();
        awaitSubscribers(room, 1);

        a.publish(room, ChatEvent.typing("ana")).block();

        assertThat(received.get(5, TimeUnit.SECONDS)).singleElement().satisfies(e -> assertThat(is(e, "typing", "ana")).isTrue());
    }

    @Test
    void messageEventSurvivesTheJsonRoundTrip() throws Exception {
        String room = room();
        var bus = newInstanceBus();
        var received = bus.receive(room).take(1).collectList().toFuture();
        awaitSubscribers(room, 1);

        var msg = new Message("id1", room, "ana", "olá, ação!", Instant.parse("2026-01-01T10:00:00Z"));
        bus.publish(room, ChatEvent.message(msg)).block();

        ChatEvent e = received.get(5, TimeUnit.SECONDS).getFirst();
        assertThat(e.type()).isEqualTo("message");
        assertThat(e.data()).isEqualTo(Map.of("id", "id1", "room", room, "user", "ana", "text", "olá, ação!",
                "createdAt", "2026-01-01T10:00:00Z"));
    }

    @Test
    void eventsOfOtherRoomsAreNotReceived() throws Exception {
        String mine = room(), other = room();
        var bus = newInstanceBus();

        StepVerifier.create(bus.receive(mine))
                .then(() -> {
                    try {
                        awaitSubscribers(mine, 1);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    bus.publish(other, ChatEvent.typing("bia")).block();
                })
                .expectNoEvent(Duration.ofMillis(300))
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void malformedPayloadIsDiscardedAndTheFeedKeepsWorking() throws Exception {
        String room = room();
        var bus = newInstanceBus();
        var received = bus.receive(room).take(1).collectList().toFuture();
        awaitSubscribers(room, 1);

        redis.convertAndSend("chat:" + room, "isto não é json").block();
        redis.convertAndSend("chat:" + room, "{\"sem\":\"campos conhecidos\", \"type\": [1,2}").block();
        bus.publish(room, ChatEvent.joined("ana")).block();

        assertThat(received.get(5, TimeUnit.SECONDS)).singleElement().satisfies(e -> assertThat(is(e, "joined", "ana")).isTrue());
    }

    @Test
    void presenceCountsConnectionsAcrossInstances() {
        String room = room();
        var a = newInstanceBus();
        var b = newInstanceBus();

        assertThat(a.join(room, "ana").block()).isTrue();   // primeira conexão da ana, em qualquer instância
        assertThat(b.join(room, "ana").block()).isFalse();  // segunda, em outra instância
        assertThat(b.join(room, "bia").block()).isTrue();
        assertThat(a.online(room).block()).containsExactlyInAnyOrder("ana", "bia");

        assertThat(a.leave(room, "ana").block()).isFalse(); // ainda resta a conexão da outra instância
        assertThat(b.online(room).block()).containsExactlyInAnyOrder("ana", "bia");
        assertThat(b.leave(room, "ana").block()).isTrue();
        assertThat(a.leave(room, "bia").block()).isTrue();
        assertThat(a.online(room).block()).isEmpty();
    }

    @Test
    void unbalancedLeaveLeavesNoNegativeCounterBehind() {
        String room = room();
        var bus = newInstanceBus();

        bus.leave(room, "fantasma").block();

        assertThat(bus.online(room).block()).isEmpty();
        assertThat(bus.join(room, "fantasma").block()).isTrue(); // contagem recomeça do zero, não de -1
    }

    // ---- ChatService em duas "instâncias" falando pelo Redis ----

    @Test
    void messageSentOnOneInstanceReachesClientsOfAnother() throws Exception {
        String room = room();
        var instanceA = newInstance();
        var instanceB = newInstance();
        var onB = instanceB.stream(room, "bia").filter(e -> e.type().equals("message")).take(1).collectList().toFuture();
        awaitSubscribers(room, 1);

        instanceA.send(room, "ana", "oi da A").block();

        assertThat(onB.get(5, TimeUnit.SECONDS)).singleElement().satisfies(e -> assertThat(isMessage(e, "oi da A")).isTrue());
    }

    @Test
    void presenceAndTypingAreSharedAcrossInstances() throws Exception {
        String room = room();
        var instanceA = newInstance();
        var instanceB = newInstance();

        StepVerifier.create(instanceA.stream(room, "ana"))
                .assertNext(e -> assertThat(e.type()).isEqualTo("presence"))
                .then(() -> {
                    try {
                        awaitSubscribers(room, 1);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    connections.add(instanceB.stream(room, "bia").subscribe()); // bia conecta na outra instância
                })
                .assertNext(e -> assertThat(is(e, "joined", "bia")).isTrue())
                .then(() -> instanceB.typing(room, "bia").block())
                .assertNext(e -> assertThat(is(e, "typing", "bia")).isTrue())
                .then(() -> connections.getFirst().dispose())
                .assertNext(e -> assertThat(is(e, "left", "bia")).isTrue())
                .thenCancel()
                .verify(Duration.ofSeconds(15));
    }

    @Test
    void newcomerOnAnotherInstanceSeesWhoIsAlreadyOnline() throws Exception {
        String room = room();
        var instanceA = newInstance();
        var instanceB = newInstance();
        connections.add(instanceA.stream(room, "ana").subscribe());
        awaitSubscribers(room, 1);

        StepVerifier.create(instanceB.stream(room, "bia"))
                .assertNext(e -> {
                    assertThat(e.type()).isEqualTo("presence");
                    assertThat(e.data()).isEqualTo(Map.of("users", List.of("ana", "bia")));
                })
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void leavingCleansRedisState() throws Exception {
        String room = room();
        var instance = newInstance();
        var connection = instance.stream(room, "ana").subscribe();
        awaitSubscribers(room, 1);

        connection.dispose();
        awaitSubscribers(room, 0); // desinscreveu do canal

        assertThat(redis.opsForHash().keys("presence:" + room).collectList().block()).isEmpty();
        assertThat(instance.activeRooms()).isZero();
    }
}
