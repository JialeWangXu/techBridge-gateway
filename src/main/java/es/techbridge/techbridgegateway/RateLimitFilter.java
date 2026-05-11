package es.techbridge.techbridgegateway;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RateLimitFilter implements WebFilter {

    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    private static final Set<String> PROTECTED_ENDPOINTS = Set.of(
            "/api/techbridge-user/login",
            "/api/techbridge-user/users"
    );

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, @NonNull WebFilterChain chain) {

        String path = exchange.getRequest().getURI().getPath();

        if (!PROTECTED_ENDPOINTS.contains(path)) {
            return chain.filter(exchange);
        }

        String ip = getClientIP(exchange);

        String key = ip + ":" + path;

        Bucket bucket = buckets.computeIfAbsent(
            key,
            this::createNewBucket
        );

        if (bucket.tryConsume(1)) {
            return chain.filter(exchange);
        }

        if (bucket.tryConsume(1)) {
            return chain.filter(exchange);
        }

        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        exchange.getResponse().getHeaders()
                .setContentType(MediaType.APPLICATION_JSON);

        String body = """
            {
                "message": "Se ha intentado demasiadas veces. Inténtalo de nuevo más tarde."
            }
            """;

        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

        return exchange.getResponse()
            .writeWith(
                Mono.just(
                    exchange.getResponse()
                        .bufferFactory()
                        .wrap(bytes)
                )
            );
    }

    private Bucket createNewBucket(String key) {

        Bandwidth limit;

        if (key.contains("/auth/register")) {
            limit = Bandwidth.builder()
                .capacity(3)
                .refillIntervally(3, Duration.ofMinutes(1))
                .build();

        } else {
            limit = Bandwidth.builder()
                .capacity(5)
                .refillIntervally(5, Duration.ofMinutes(1))
                .build();
        }

        return Bucket.builder()
                .addLimit(limit)
                .build();
    }

    private String getClientIP(ServerWebExchange exchange) {

        String xForwardedFor = exchange.getRequest()
                .getHeaders()
                .getFirst("X-Forwarded-For");

        if (xForwardedFor != null && !xForwardedFor.isEmpty()) {
            return xForwardedFor.split(",")[0];
        }

        if (exchange.getRequest().getRemoteAddress() == null) {
            return "unknown";
        }

        return exchange.getRequest()
                .getRemoteAddress()
                .getAddress()
                .getHostAddress();
    }
}
