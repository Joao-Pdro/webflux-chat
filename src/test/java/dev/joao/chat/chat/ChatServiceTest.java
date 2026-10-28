package dev.joao.chat.chat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChatServiceTest {

    private final MessageRepository repo = mock(MessageRepository.class);
    private final ChatService chat = new ChatService(repo, new LocalChatBus());

    @BeforeEach
    void stubRepository() {
        when(repo.save(any())).thenAnswer(i -> Mono.just(i.getArgument(0)));
        when(repo.findTop50ByRoomOrderByCreatedAtDesc(any())).thenReturn(Flux.empty());
    }

    private static boolean is(ChatEvent e, String type, String user) {
        return e.type().equals(type) && e.data() instanceof Map<?, ?> d && user.equals(d.get("user"));
    }

    private static boolean isMessage(ChatEvent e, String text) {
        return e.data() instanceof Message m && e.type().equals("message") && m.text().equals(text);
    }

    @Test
    void messageReachesEverySubscriberOfTheRoom() throws Exception {
        var a = chat.stream("geral", "ana").filter(e -> e.type().equals("message")).take(1).collectList().toFuture();
        var b = chat.stream("geral", "bia").filter(e -> e.type().equals("message")).take(1).collectList().toFuture();

        chat.send("geral", "ana", "oi").block();

        assertThat(a.get(1, TimeUnit.SECONDS)).hasSize(1).allMatch(e -> isMessage(e, "oi"));
        assertThat(b.get(1, TimeUnit.SECONDS)).hasSize(1).allMatch(e -> isMessage(e, "oi"));
    }

    @Test
    void otherRoomDoesNotReceive() {
        StepVerifier.create(chat.stream("a", "ana"))
                .expectNextMatches(e -> e.type().equals("presence"))
                .then(() -> {
                    chat.send("b", "ana", "oi").block();
                    chat.typing("b", "ana").block();
                })
                .expectNoEvent(Duration.ofMillis(200))
                .thenCancel()
                .verify();
    }

    @Test
    void historyComesInChronologicalOrder() {
        Instant t = Instant.parse("2026-01-01T00:00:00Z");
        var m1 = new Message("1", "geral", "ana", "um", t);
        var m2 = new Message("2", "geral", "ana", "dois", t.plusSeconds(1));
        var m3 = new Message("3", "geral", "ana", "tres", t.plusSeconds(2));
        when(repo.findTop50ByRoomOrderByCreatedAtDesc("geral")).thenReturn(Flux.just(m3, m2, m1));

        StepVerifier.create(chat.history("geral"))
                .expectNext(m1, m2, m3)
                .verifyComplete();
    }

    @Test
    void streamDeliversHistoryThenPresenceThenLive() {
        var old = new Message("1", "geral", "ana", "antiga", Instant.now());
        when(repo.findTop50ByRoomOrderByCreatedAtDesc("geral")).thenReturn(Flux.just(old));

        StepVerifier.create(chat.stream("geral", "ana"))
                .expectNextMatches(e -> isMessage(e, "antiga"))
                .assertNext(e -> {
                    assertThat(e.type()).isEqualTo("presence");
                    assertThat(e.data()).isEqualTo(Map.of("users", List.of("ana")));
                })
                .then(() -> chat.send("geral", "bia", "nova").block())
                .expectNextMatches(e -> isMessage(e, "nova"))
                .thenCancel()
                .verify();
    }

    @Test
    void presenceIsAnnouncedOnlyForFirstAndLastConnectionOfAUser() {
        Disposable[] bia = new Disposable[2];

        StepVerifier.create(chat.stream("geral", "ana"))
                .expectNextMatches(e -> e.type().equals("presence"))
                .then(() -> bia[0] = chat.stream("geral", "bia").subscribe())
                .expectNextMatches(e -> is(e, "joined", "bia"))
                .then(() -> bia[1] = chat.stream("geral", "bia").subscribe()) // segunda aba: sem novo "joined"
                .expectNoEvent(Duration.ofMillis(100))
                .then(() -> bia[0].dispose()) // ainda resta uma aba: sem "left"
                .expectNoEvent(Duration.ofMillis(100))
                .then(() -> bia[1].dispose())
                .expectNextMatches(e -> is(e, "left", "bia"))
                .thenCancel()
                .verify();
    }

    @Test
    void typingIsDeliveredToOthersAndNeverPersisted() {
        StepVerifier.create(chat.stream("geral", "ana"))
                .expectNextMatches(e -> e.type().equals("presence"))
                .then(() -> chat.typing("geral", "bia").block())
                .expectNextMatches(e -> is(e, "typing", "bia"))
                .thenCancel()
                .verify();

        verify(repo, never()).save(any());
    }

    @Test
    void roomIsRemovedWhenTheLastUserLeaves() {
        Disposable ana = chat.stream("geral", "ana").subscribe();
        Disposable bia = chat.stream("geral", "bia").subscribe();
        assertThat(chat.activeRooms()).isEqualTo(1);

        ana.dispose();
        assertThat(chat.activeRooms()).isEqualTo(1);
        bia.dispose();
        assertThat(chat.activeRooms()).isZero();
    }

    // Duas instâncias do serviço compartilhando o mesmo bus: é o contrato que o Redis precisa cumprir.
    @Test
    void messageSentOnOneInstanceReachesSubscribersOfAnother() throws Exception {
        var bus = new LocalChatBus();
        var instanceA = new ChatService(repo, bus);
        var instanceB = new ChatService(repo, bus);

        var onB = instanceB.stream("geral", "bia").filter(e -> e.type().equals("message")).take(1).collectList().toFuture();
        instanceA.send("geral", "ana", "oi da instancia A").block();

        assertThat(onB.get(1, TimeUnit.SECONDS)).hasSize(1).allMatch(e -> isMessage(e, "oi da instancia A"));
    }

    @Test
    void presenceIsSharedAcrossInstances() {
        var bus = new LocalChatBus();
        var instanceA = new ChatService(repo, bus);
        var instanceB = new ChatService(repo, bus);
        Disposable[] biaTabs = new Disposable[2];

        StepVerifier.create(instanceA.stream("geral", "ana"))
                .assertNext(e -> assertThat(e.data()).isEqualTo(Map.of("users", List.of("ana"))))
                .then(() -> biaTabs[0] = instanceB.stream("geral", "bia").subscribe())
                .expectNextMatches(e -> is(e, "joined", "bia")) // ana (em A) vê quem entrou em B
                .then(() -> biaTabs[1] = instanceA.stream("geral", "bia").subscribe()) // mesma pessoa, outra instância
                .expectNoEvent(Duration.ofMillis(100)) // continua sendo uma presença só
                .then(() -> biaTabs[0].dispose())
                .expectNoEvent(Duration.ofMillis(100))
                .then(() -> biaTabs[1].dispose())
                .expectNextMatches(e -> is(e, "left", "bia"))
                .thenCancel()
                .verify();

        // quem conecta depois, em outra instância, já recebe a lista de online completa
        Disposable ana = instanceA.stream("geral", "ana").subscribe();
        StepVerifier.create(instanceB.stream("geral", "cid"))
                .assertNext(e -> assertThat(e.data()).isEqualTo(Map.of("users", List.of("ana", "cid"))))
                .thenCancel()
                .verify();
    }

    @Test
    void publishFailureDoesNotFailASavedMessage() {
        var failing = mock(ChatBus.class);
        when(failing.publish(any(), any())).thenReturn(Mono.error(new IllegalStateException("redis fora")));
        var service = new ChatService(repo, failing);

        StepVerifier.create(service.send("geral", "ana", "oi"))
                .assertNext(m -> assertThat(m.text()).isEqualTo("oi"))
                .verifyComplete();
        verify(repo).save(any());
    }

    @Test
    void userDoesNotReceiveTheirOwnJoinedEventButSeesOthers() {
        var bus = new LocalChatBus();
        var service = new ChatService(repo, bus);

        StepVerifier.create(service.stream("geral", "ana"))
                .expectNextMatches(e -> e.type().equals("presence"))
                .then(() -> bus.publish("geral", ChatEvent.joined("ana")).block()) // eco do próprio join (vindo do Redis)
                .expectNoEvent(Duration.ofMillis(100))
                .then(() -> bus.publish("geral", ChatEvent.joined("bia")).block())
                .expectNextMatches(e -> is(e, "joined", "bia"))
                .thenCancel()
                .verify();
    }

    @Test
    void slowSubscriberKeepsOnlyTheNewest100Events() {
        StepVerifier.create(chat.stream("geral", "ana"), 1) // pede só 1: o "presence"
                .expectNextMatches(e -> e.type().equals("presence"))
                .then(() -> IntStream.range(0, 150).forEach(i -> chat.send("geral", "bia", "m" + i).block()))
                .thenRequest(100)
                .assertNext(e -> assertThat(isMessage(e, "m50")).isTrue())
                .expectNextCount(98)
                .assertNext(e -> assertThat(isMessage(e, "m149")).isTrue())
                .thenCancel()
                .verify();
    }
}
