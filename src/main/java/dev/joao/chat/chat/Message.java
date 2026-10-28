package dev.joao.chat.chat;

import java.time.Instant;

public record Message(String room, String user, String text, Instant createdAt) {
}
