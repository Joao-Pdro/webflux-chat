package dev.joao.chat.auth;

import java.time.Duration;
import java.time.Instant;

import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RestController
@RequestMapping("/auth")
public class AuthController {

    private final UserRepository users;
    private final PasswordEncoder encoder;
    private final JwtEncoder jwt;
    private final Duration ttl;

    public AuthController(UserRepository users, PasswordEncoder encoder, JwtEncoder jwt,
                          @Value("${app.jwt.ttl}") Duration ttl) {
        this.users = users;
        this.encoder = encoder;
        this.jwt = jwt;
        this.ttl = ttl;
    }

    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    Mono<Void> register(@Valid @RequestBody AuthRequest req) {
        return blocking(() -> encoder.encode(req.password()))
                .flatMap(hash -> users.save(new User(null, req.username(), hash)))
                .onErrorMap(DuplicateKeyException.class,
                        e -> new ResponseStatusException(HttpStatus.CONFLICT, "username already taken"))
                .then();
    }

    @PostMapping("/login")
    Mono<TokenResponse> login(@Valid @RequestBody AuthRequest req) {
        return users.findByUsername(req.username())
                .filterWhen(u -> blocking(() -> encoder.matches(req.password(), u.passwordHash())))
                .map(u -> new TokenResponse(token(u.username())))
                // mesma resposta para usuário inexistente e senha errada
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "invalid credentials")));
    }

    // BCrypt é CPU-bound de propósito: fora das threads do event loop
    private static <T> Mono<T> blocking(java.util.concurrent.Callable<T> work) {
        return Mono.fromCallable(work).subscribeOn(Schedulers.boundedElastic());
    }

    private String token(String username) {
        Instant now = Instant.now();
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .subject(username)
                .issuedAt(now)
                .expiresAt(now.plus(ttl))
                .build();
        JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).build();
        return jwt.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }
}
