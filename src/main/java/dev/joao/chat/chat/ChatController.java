package dev.joao.chat.chat;

import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class ChatController {

    private final ChatService chat;

    public ChatController(ChatService chat) {
        this.chat = chat;
    }

    @GetMapping(value = "/rooms/{room}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<Message> stream(@PathVariable String room) {
        return chat.stream(room);
    }

    @PostMapping("/rooms/{room}/messages")
    Message send(@PathVariable String room, @Valid @RequestBody SendRequest req) {
        return chat.send(room, req.user(), req.text());
    }
}
