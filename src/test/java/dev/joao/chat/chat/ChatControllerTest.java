package dev.joao.chat.chat;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import dev.joao.chat.auth.SecurityConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt;

@WebFluxTest(controllers = ChatController.class)
@Import(SecurityConfig.class)
class ChatControllerTest {

    @Autowired WebTestClient client;
    @MockitoBean ChatService chat;

    private WebTestClient asAna() {
        return client.mutateWith(mockJwt().jwt(j -> j.subject("ana")));
    }

    @Test
    void sseEventsCarryTheirTypeAsEventNameAndJsonAsData() {
        var msg = new Message("1", "geral", "ana", "oi", Instant.parse("2026-01-01T00:00:00Z"));
        when(chat.stream("geral", "ana")).thenReturn(Flux.just(
                ChatEvent.message(msg), ChatEvent.presence(List.of("bia", "ana")), ChatEvent.joined("bia"),
                ChatEvent.left("bia"), ChatEvent.typing("bia")));

        Flux<ServerSentEvent<String>> events = asAna().get().uri("/rooms/geral/stream")
                .accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(new ParameterizedTypeReference<ServerSentEvent<String>>() { })
                .getResponseBody();

        StepVerifier.create(events)
                .assertNext(e -> {
                    assertThat(e.event()).isEqualTo("message");
                    assertThat(e.data())
                            .contains("\"id\":\"1\"", "\"user\":\"ana\"", "\"text\":\"oi\"", "\"room\":\"geral\"");
                })
                .assertNext(e -> {
                    assertThat(e.event()).isEqualTo("presence");
                    assertThat(e.data()).isEqualTo("{\"users\":[\"ana\",\"bia\"]}"); // ordenado
                })
                .assertNext(e -> {
                    assertThat(e.event()).isEqualTo("joined");
                    assertThat(e.data()).isEqualTo("{\"user\":\"bia\"}");
                })
                .assertNext(e -> assertThat(e.event()).isEqualTo("left"))
                .assertNext(e -> assertThat(e.event()).isEqualTo("typing"))
                .verifyComplete();
    }

    @Test
    void blankTextIs400() {
        asAna().post().uri("/rooms/geral/messages").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("text", "   ")).exchange().expectStatus().isBadRequest();
    }

    @Test
    void textOver1000CharsIs400() {
        asAna().post().uri("/rooms/geral/messages").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("text", "x".repeat(1001))).exchange().expectStatus().isBadRequest();
    }

    @Test
    void missingBodyIs400() {
        asAna().post().uri("/rooms/geral/messages").exchange().expectStatus().isBadRequest();
    }
}
