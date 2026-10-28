package dev.joao.chat;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import dev.joao.chat.chat.MessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/** Aplicação inteira (perfil padrão, bus local) com Mongo real, falando HTTP de verdade. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(MongoContainerConfig.class)
class ChatIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final ParameterizedTypeReference<ServerSentEvent<String>> SSE = new ParameterizedTypeReference<>() { };

    @Value("${local.server.port}") int port;
    @Autowired MessageRepository messages;
    WebTestClient client;

    @BeforeEach
    void bindClient() {
        client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).responseTimeout(TIMEOUT).build();
    }

    private static String unique(String prefix) {
        return prefix + UUID.randomUUID().toString().substring(0, 8);
    }

    private void register(String user, String password) {
        client.post().uri("/auth/register").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("username", user, "password", password)).exchange().expectStatus().isCreated();
    }

    private String login(String user, String password) {
        return client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("username", user, "password", password)).exchange()
                .expectStatus().isOk()
                .returnResult(Map.class).getResponseBody().blockFirst().get("token").toString();
    }

    /** Cadastra e já devolve o token. */
    private String newUser(String user) {
        register(user, "secret1");
        return login(user, "secret1");
    }

    private WebTestClient.ResponseSpec post(String token, String room, String text) {
        return client.post().uri("/rooms/{r}/messages", room).header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON).bodyValue(Map.of("text", text)).exchange();
    }

    private Flux<ServerSentEvent<String>> stream(String token, String room) {
        return client.get().uri("/rooms/{r}/stream", room).header("Authorization", "Bearer " + token)
                .accept(MediaType.TEXT_EVENT_STREAM).exchange().expectStatus().isOk()
                .returnResult(SSE).getResponseBody();
    }

    @Test
    void registrationRules() {
        String user = unique("u");
        register(user, "secret1");

        client.post().uri("/auth/register").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("username", user, "password", "secret1")).exchange()
                .expectStatus().isEqualTo(409);
        client.post().uri("/auth/register").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("username", unique("u"), "password", "123")).exchange()
                .expectStatus().isBadRequest();
        client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("username", user, "password", "errada1")).exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void endpointsNeedAToken() {
        client.get().uri("/rooms/x/stream").exchange().expectStatus().isUnauthorized();
        client.post().uri("/rooms/x/messages").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("text", "oi")).exchange().expectStatus().isUnauthorized();
        client.post().uri("/rooms/x/typing").exchange().expectStatus().isUnauthorized();
    }

    @Test
    void newcomerGetsHistoryThenPresenceThenLiveMessages() {
        String room = unique("sala-"), user = unique("u");
        String token = newUser(user);
        post(token, room, "primeira").expectStatus().isOk();
        post(token, room, "segunda").expectStatus().isOk();

        StepVerifier.create(stream(token, room))
                .assertNext(e -> assertThat(e).satisfies(ev -> {
                    assertThat(ev.event()).isEqualTo("message");
                    assertThat(ev.data()).contains("\"text\":\"primeira\"", "\"user\":\"" + user + "\"");
                }))
                .assertNext(e -> assertThat(e.data()).contains("\"text\":\"segunda\"")) // mesma ordem em que foram enviadas
                .assertNext(e -> {
                    assertThat(e.event()).isEqualTo("presence");
                    assertThat(e.data()).isEqualTo("{\"users\":[\"" + user + "\"]}");
                })
                .then(() -> post(token, room, "ao vivo").expectStatus().isOk())
                .assertNext(e -> {
                    assertThat(e.event()).isEqualTo("message");
                    assertThat(e.data()).contains("\"text\":\"ao vivo\"");
                })
                .thenCancel()
                .verify(TIMEOUT);
    }

    @Test
    void authorIsTheTokenOwnerEvenIfTheBodyClaimsOtherwise() {
        String room = unique("sala-"), user = unique("u");
        String token = newUser(user);

        client.post().uri("/rooms/{r}/messages", room).header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON).bodyValue(Map.of("text", "oi", "user", "impostor")).exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.user").isEqualTo(user);
        assertThat(messages.findTop50ByRoomOrderByCreatedAtDesc(room).collectList().block())
                .singleElement().satisfies(m -> assertThat(m.user()).isEqualTo(user));
    }

    @Test
    void anotherRoomDoesNotSeeMessages() {
        String roomA = unique("a-"), roomB = unique("b-"), user = unique("u");
        String token = newUser(user);
        post(token, roomA, "so na A").expectStatus().isOk();

        StepVerifier.create(stream(token, roomB))
                .assertNext(e -> assertThat(e.event()).isEqualTo("presence")) // nenhuma mensagem antes dela
                .thenCancel()
                .verify(TIMEOUT);
    }

    @Test
    void typingIsDeliveredToOthersButNeverStored() {
        String room = unique("sala-");
        String ana = unique("a"), bia = unique("b");
        String tokenAna = newUser(ana), tokenBia = newUser(bia);

        StepVerifier.create(stream(tokenAna, room))
                .assertNext(e -> assertThat(e.event()).isEqualTo("presence"))
                .then(() -> client.post().uri("/rooms/{r}/typing", room).header("Authorization", "Bearer " + tokenBia)
                        .exchange().expectStatus().isNoContent())
                .assertNext(e -> {
                    assertThat(e.event()).isEqualTo("typing");
                    assertThat(e.data()).isEqualTo("{\"user\":\"" + bia + "\"}");
                })
                .thenCancel()
                .verify(TIMEOUT);

        assertThat(messages.findTop50ByRoomOrderByCreatedAtDesc(room).count().block()).isZero();
    }

    @Test
    void presenceFollowsConnectionsIncludingDisconnects() {
        String room = unique("sala-");
        String ana = unique("a"), bia = unique("b");
        String tokenAna = newUser(ana), tokenBia = newUser(bia);
        Disposable[] biaConnection = new Disposable[1];

        StepVerifier.create(stream(tokenAna, room))
                .assertNext(e -> assertThat(e.data()).isEqualTo("{\"users\":[\"" + ana + "\"]}"))
                .then(() -> biaConnection[0] = stream(tokenBia, room).subscribe())
                .assertNext(e -> {
                    assertThat(e.event()).isEqualTo("joined");
                    assertThat(e.data()).isEqualTo("{\"user\":\"" + bia + "\"}");
                })
                .then(() -> biaConnection[0].dispose()) // o cliente fecha a conexão: o servidor precisa perceber
                .assertNext(e -> {
                    assertThat(e.event()).isEqualTo("left");
                    assertThat(e.data()).isEqualTo("{\"user\":\"" + bia + "\"}");
                })
                .thenCancel()
                .verify(TIMEOUT);
    }
}
