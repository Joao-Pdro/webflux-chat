package dev.joao.chat.chat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import dev.joao.chat.MongoContainerConfig;
import dev.joao.chat.auth.User;
import dev.joao.chat.auth.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.mongodb.test.autoconfigure.DataMongoTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.IndexInfo;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import static org.assertj.core.api.Assertions.assertThat;

@DataMongoTest
@Import(MongoContainerConfig.class)
class MessageRepositoryTest {

    @Autowired MessageRepository messages;
    @Autowired UserRepository users;
    @Autowired ReactiveMongoTemplate template;

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    void top50ReturnsTheNewestFirstAndOnlyFromThatRoom() {
        String room = unique("geral"), other = unique("outra");
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        var inRoom = IntStream.range(0, 60)
                .mapToObj(i -> new Message(null, room, "ana", "m" + i, base.plusSeconds(i))).toList();
        var inOther = IntStream.range(0, 5)
                .mapToObj(i -> new Message(null, other, "bia", "x" + i, base.plusSeconds(i))).toList();
        messages.saveAll(Flux.fromIterable(inRoom).concatWith(Flux.fromIterable(inOther))).blockLast();

        List<Message> top = messages.findTop50ByRoomOrderByCreatedAtDesc(room).collectList().block();

        assertThat(top).hasSize(50)
                .allMatch(m -> m.room().equals(room))
                .extracting(Message::text)
                .first().isEqualTo("m59");
        assertThat(top.get(49).text()).isEqualTo("m10"); // as 10 mais antigas ficam de fora
    }

    @Test
    void savedMessageGetsAnIdAndKeepsItsFields() {
        String room = unique("geral");
        Instant at = Instant.parse("2026-01-01T10:00:00.123Z");

        Message saved = messages.save(new Message(null, room, "ana", "oi", at)).block();

        assertThat(saved.id()).isNotBlank();
        Message loaded = messages.findById(saved.id()).block();
        assertThat(loaded).isEqualTo(saved);
        assertThat(loaded.createdAt()).isEqualTo(at); // o Mongo guarda milissegundos
    }

    @Test
    void emptyRoomHasNoHistory() {
        StepVerifier.create(messages.findTop50ByRoomOrderByCreatedAtDesc(unique("vazia")))
                .verifyComplete();
    }

    @Test
    void usernamesAreUnique() {
        // o índice é criado de forma assíncrona na subida do contexto: espera existir antes de testar
        IndexInfo index = template.indexOps(User.class).getIndexInfo()
                .filter(i -> i.getName().equals("username")).single()
                .retryWhen(Retry.fixedDelay(30, Duration.ofMillis(100))).block();
        assertThat(index.isUnique()).isTrue();

        String name = unique("u");
        users.save(new User(null, name, "hash")).block();

        StepVerifier.create(users.save(new User(null, name, "outro-hash")))
                .expectError(DuplicateKeyException.class)
                .verify();
        assertThat(users.findByUsername(name).block().passwordHash()).isEqualTo("hash");
    }
}
