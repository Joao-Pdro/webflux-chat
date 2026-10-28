package dev.joao.chat.chat;

import java.security.Principal;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
public class ChatController {

    private final ChatService chat;

    public ChatController(ChatService chat) {
        this.chat = chat;
    }

    @GetMapping(value = "/rooms/{room}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<ServerSentEvent<Object>> stream(@PathVariable String room, Principal principal) {
        return chat.stream(room, principal.getName())
                .map(e -> ServerSentEvent.builder().event(e.type()).data(e.data()).build());
    }

    @PostMapping("/rooms/{room}/messages")
    Mono<Message> send(@PathVariable String room, @Valid @RequestBody SendRequest req, Principal principal) {
        return chat.send(room, principal.getName(), req.text());
    }

    @PostMapping("/rooms/{room}/typing")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    Mono<Void> typing(@PathVariable String room, Principal principal) {
        return chat.typing(room, principal.getName());
    }
}
