package dev.joao.chat.chat;

import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
@Document("messages")
public record Message(String id, String room, String user, String text, Instant createdAt) {
}
