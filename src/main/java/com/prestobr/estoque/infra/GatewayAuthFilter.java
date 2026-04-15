package com.prestobr.estoque.infra;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class GatewayAuthFilter extends OncePerRequestFilter {

    private static final String GATEWAY_SECRET_HEADER = "X-Gateway-Secret";
    private static final String USER_ID_HEADER = "X-User-Id";
    private static final String USER_ROLES_HEADER = "X-User-Roles";

    @Value("${gateway.secret}")
    private String gatewaySecret;

    @PostConstruct
    public void init() {
        log.info("=== GatewayAuthFilter loaded! gateway.secret present: {}", gatewaySecret != null);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        String path = request.getRequestURI();

        // 🔹 IMPORTANTE — permitir preflight CORS
        if ("OPTIONS".equalsIgnoreCase(request.getMethod())) {
            filterChain.doFilter(request, response);
            return;
        }

        if (isPublicPath(path)) {
            log.info("=== Public path, allowing");
            filterChain.doFilter(request, response);
            return;
        }

        String secret = request.getHeader(GATEWAY_SECRET_HEADER);
        log.info("=== GatewayAuthFilter: path={}, secret present={}", path, secret != null);

        if (!isValidGatewaySecret(secret)) {
            log.warn("=== Invalid secret! Received: {}, Expected: {}", secret, gatewaySecret);
            sendUnauthorizedResponse(response);
            return;
        }

        log.info("=== Secret valid, configuring security context");
        try {
            configureSecurityContext(request);
            filterChain.doFilter(request, response);
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    private boolean isPublicPath(String path) {
        return path.startsWith("/actuator")
                || path.startsWith("/swagger-ui")
                || path.startsWith("/swagger-ui.html")
                || path.startsWith("/v3/api-docs");
    }

    private boolean isValidGatewaySecret(String secret) {
        return secret != null && secret.equals(gatewaySecret);
    }

    private void configureSecurityContext(HttpServletRequest request) {
        String userId = request.getHeader(USER_ID_HEADER);
        String roles = request.getHeader(USER_ROLES_HEADER);

        if (userId != null) {

            List<SimpleGrantedAuthority> authorities = parseRoles(roles);

            UsernamePasswordAuthenticationToken authentication =
                    new UsernamePasswordAuthenticationToken(userId, null, authorities);

            SecurityContextHolder.getContext().setAuthentication(authentication);
        } else {
            List<SimpleGrantedAuthority> authorities = List.of(
                    new SimpleGrantedAuthority("ROLE_ADMIN")
            );
            UsernamePasswordAuthenticationToken authentication =
                    new UsernamePasswordAuthenticationToken("system", null, authorities);
            SecurityContextHolder.getContext().setAuthentication(authentication);
        }
    }

    private List<SimpleGrantedAuthority> parseRoles(String roles) {

        List<SimpleGrantedAuthority> authorities = new ArrayList<>();

        if (roles != null && !roles.isEmpty()) {
            for (String role : roles.split(",")) {

                String trimmedRole = role.trim();

                if (!trimmedRole.isEmpty()) {
                    authorities.add(new SimpleGrantedAuthority("ROLE_" + trimmedRole));
                }
            }
        }

        return authorities;
    }

    private void sendUnauthorizedResponse(HttpServletResponse response) throws IOException {

        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");

        response.getWriter().write(
                "{\"error\": \"Unauthorized\", \"message\": \"Invalid or missing gateway authentication\"}"
        );
    }
}