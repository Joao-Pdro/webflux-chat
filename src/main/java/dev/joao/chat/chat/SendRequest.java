package dev.joao.chat.chat;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record SendRequest(@NotBlank @Size(max = 50) String user, @NotBlank @Size(max = 1000) String text) {
}
