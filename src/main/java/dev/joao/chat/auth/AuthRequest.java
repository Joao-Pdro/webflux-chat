package dev.joao.chat.auth;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record AuthRequest(@NotBlank @Size(min = 3, max = 30) String username,
                          @NotBlank @Size(min = 6, max = 72) String password) {
}
