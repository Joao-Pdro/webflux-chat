package dev.joao.chat.auth;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

import javax.crypto.spec.SecretKeySpec;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import dev.joao.chat.chat.ChatController;
import dev.joao.chat.chat.ChatService;
import dev.joao.chat.chat.Message;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt;

@WebFluxTest(controllers = {ChatController.class, AuthController.class})
@Import(SecurityConfig.class)
class SecurityTest {

    @Autowired WebTestClient client;
    @Autowired JwtEncoder jwtEncoder;
    @MockitoBean ChatService chat;
    @MockitoBean UserRepository users;

    @Test
    void streamWithoutTokenIs401() {
        client.get().uri("/rooms/geral/stream").exchange().expectStatus().isUnauthorized();
    }

    @Test
    void sendWithoutTokenIs401() {
        client.post().uri("/rooms/geral/messages").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("text", "oi")).exchange().expectStatus().isUnauthorized();
    }

    @Test
    void authorComesFromTokenSubjectNotFromBody() {
        when(chat.send("geral", "ana", "oi"))
                .thenReturn(Mono.just(new Message("1", "geral", "ana", "oi", Instant.now())));

        client.mutateWith(mockJwt().jwt(j -> j.subject("ana")))
                .post().uri("/rooms/geral/messages").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("text", "oi", "user", "impostor")).exchange()
                .expectStatus().isOk();

        verify(chat).send("geral", "ana", "oi");
    }

    @Test
    void loginReturnsATokenThatOpensTheStream() {
        when(users.findByUsername("ana")).thenReturn(
                Mono.just(new User("1", "ana", new BCryptPasswordEncoder().encode("secret1"))));
        when(chat.stream("geral", "ana")).thenReturn(Flux.empty());

        String token = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("username", "ana", "password", "secret1")).exchange()
                .expectStatus().isOk()
                .returnResult(TokenResponse.class).getResponseBody().blockFirst().token();

        client.get().uri("/rooms/geral/stream").header("Authorization", "Bearer " + token)
                .accept(MediaType.TEXT_EVENT_STREAM).exchange().expectStatus().isOk();
    }

    @Test
    void wrongPasswordAndUnknownUserBothGet401() {
        when(users.findByUsername("ana")).thenReturn(
                Mono.just(new User("1", "ana", new BCryptPasswordEncoder().encode("secret1"))));
        when(users.findByUsername("zed")).thenReturn(Mono.empty());

        client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("username", "ana", "password", "errada1")).exchange()
                .expectStatus().isUnauthorized();
        client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("username", "zed", "password", "secret1")).exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void typingNeedsATokenAndUsesTheTokenSubject() {
        client.post().uri("/rooms/geral/typing").exchange().expectStatus().isUnauthorized();

        when(chat.typing("geral", "ana")).thenReturn(Mono.empty());

        client.mutateWith(mockJwt().jwt(j -> j.subject("ana")))
                .post().uri("/rooms/geral/typing").exchange().expectStatus().isNoContent();

        verify(chat).typing("geral", "ana");
    }

    @Test
    void forgedTokenIsRejected() {
        client.get().uri("/rooms/geral/stream").header("Authorization", "Bearer abc.def.ghi")
                .exchange().expectStatus().isUnauthorized();
    }

    // --- tokens: cada caso rejeitado tem um controle válido, para o 401 não vir de outro motivo ---

    private static String token(JwtEncoder encoder, String subject, Instant issuedAt, Instant expiresAt) {
        JwtClaimsSet claims = JwtClaimsSet.builder().subject(subject).issuedAt(issuedAt).expiresAt(expiresAt).build();
        return encoder.encode(JwtEncoderParameters.from(JwsHeader.with(MacAlgorithm.HS256).build(), claims)).getTokenValue();
    }

    private WebTestClient.ResponseSpec streamWith(String token) {
        return client.get().uri("/rooms/geral/stream").header("Authorization", "Bearer " + token)
                .accept(MediaType.TEXT_EVENT_STREAM).exchange();
    }

    @Test
    void freshlySignedTokenIsAccepted() {
        when(chat.stream("geral", "ana")).thenReturn(Flux.empty());
        Instant now = Instant.now();

        streamWith(token(jwtEncoder, "ana", now, now.plus(Duration.ofHours(1)))).expectStatus().isOk();
    }

    @Test
    void expiredTokenIsRejected() {
        Instant now = Instant.now();

        streamWith(token(jwtEncoder, "ana", now.minus(Duration.ofHours(2)), now.minus(Duration.ofHours(1))))
                .expectStatus().isUnauthorized();
    }

    @Test
    void tokenSignedWithAnotherKeyIsRejected() {
        var other = new NimbusJwtEncoder(new ImmutableSecret<>(
                new SecretKeySpec("outra-chave-outra-chave-outra-chave-123".getBytes(StandardCharsets.UTF_8), "HmacSHA256")));
        Instant now = Instant.now();

        streamWith(token(other, "ana", now, now.plus(Duration.ofHours(1)))).expectStatus().isUnauthorized();
    }

    @Test
    void unsignedTokenIsRejected() {
        var b64 = Base64.getUrlEncoder().withoutPadding();
        String header = b64.encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8));
        String payload = b64.encodeToString(("{\"sub\":\"ana\",\"exp\":" + (Instant.now().getEpochSecond() + 3600) + "}")
                .getBytes(StandardCharsets.UTF_8));

        streamWith(header + "." + payload + ".").expectStatus().isUnauthorized();
    }

    @Test
    void tokenWithTamperedPayloadIsRejected() {
        Instant now = Instant.now();
        String[] parts = token(jwtEncoder, "ana", now, now.plus(Duration.ofHours(1))).split("\\.");
        String forgedPayload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                ("{\"sub\":\"admin\",\"exp\":" + (now.getEpochSecond() + 3600) + "}").getBytes(StandardCharsets.UTF_8));

        streamWith(parts[0] + "." + forgedPayload + "." + parts[2]).expectStatus().isUnauthorized(); // assinatura antiga
    }
}
