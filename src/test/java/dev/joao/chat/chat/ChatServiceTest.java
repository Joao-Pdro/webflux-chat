package dev.joao.chat.chat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class ChatServiceTest {

    private final ChatService chat = new ChatService();

    @Test
    void subscribersOfTheSameRoomReceiveTheMessage() {
        var a = chat.stream("geral").take(1);
        var b = chat.stream("geral").take(1);

        StepVerifier.create(a).then(() -> chat.send("geral", "ana", "oi"))
                .expectNextMatches(m -> m.text().equals("oi") && m.user().equals("ana"))
                .verifyComplete();
        StepVerifier.create(b).then(() -> chat.send("geral", "ana", "oi2"))
                .expectNextMatches(m -> m.text().equals("oi2"))
                .verifyComplete();
    }

    @Test
    void otherRoomDoesNotReceive() {
        StepVerifier.create(chat.stream("a"))
                .then(() -> chat.send("b", "ana", "oi"))
                .expectNoEvent(Duration.ofMillis(200))
                .thenCancel()
                .verify();
    }
}
