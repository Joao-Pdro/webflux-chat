package dev.joao.chat.chat;

import java.util.Collection;
import java.util.Map;

/** Evento de uma sala; {@code type} vira o nome do evento SSE e {@code data} o corpo JSON. */
public record ChatEvent(String type, Object data) {

    public static ChatEvent message(Message m) {
        return new ChatEvent("message", m);
    }

    /** Quem já estava na sala quando o cliente conectou. */
    public static ChatEvent presence(Collection<String> users) {
        return new ChatEvent("presence", Map.of("users", users.stream().sorted().toList()));
    }

    public static ChatEvent joined(String user) {
        return new ChatEvent("joined", Map.of("user", user));
    }

    public static ChatEvent left(String user) {
        return new ChatEvent("left", Map.of("user", user));
    }

    public static ChatEvent typing(String user) {
        return new ChatEvent("typing", Map.of("user", user));
    }

    /** O evento fala deste usuário? Vale para joined/left/typing, cujo data é {"user": ...}. */
    public boolean isAbout(String user) {
        return data instanceof Map<?, ?> d && user.equals(d.get("user"));
    }
}
