package dev.joao.chat.chat;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;

public interface MessageRepository extends ReactiveMongoRepository<Message, String> {

    Flux<Message> findTop50ByRoomOrderByCreatedAtDesc(String room);
}
