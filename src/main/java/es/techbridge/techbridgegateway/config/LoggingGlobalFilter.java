package es.techbridge.techbridgegateway.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class LoggingGlobalFilter implements GlobalFilter, Ordered {

    private static final Logger logger = LoggerFactory.getLogger(LoggingGlobalFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, org.springframework.cloud.gateway.filter.GatewayFilterChain chain) {
        // Mono es para que sea reactivo, sin bloquear el hilo principal
        var request = exchange.getRequest(); // la peticion http

        logger.info(">>> {} {}  headers={}", request.getMethod(), request.getURI().getPath(), request.getHeaders());
        return chain.filter(exchange);
    }

    @Override
    public int getOrder() {
        return -1;
    } // para que sea el primer filtro que se evalua en todas las peticiones
}
