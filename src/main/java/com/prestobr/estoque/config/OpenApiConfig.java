package com.prestobr.estoque.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.TreeMap;

@Configuration
public class OpenApiConfig {

    @Value("${api.gateway.url}")
    private String gatewayUrl;

    @Bean
    public OpenAPI customOpenAPI() {

        Server server = new Server();
        server.setUrl(gatewayUrl + "/api/estoque");
        server.setDescription("API Estoque");

        return new OpenAPI()
                .servers(List.of(server))
                .addSecurityItem(new SecurityRequirement().addList("bearerAuth"))
                .addSecurityItem(new SecurityRequirement().addList("apiKey"))
                .components(new Components()
                        .addSecuritySchemes("bearerAuth", new SecurityScheme()
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("bearer")
                                .bearerFormat("JWT")
                                .description("Token JWT obtido no login"))
                        .addSecuritySchemes("apiKey", new SecurityScheme()
                                .type(SecurityScheme.Type.APIKEY)
                                .in(SecurityScheme.In.HEADER)
                                .name("Authorization")
                                .description("API Key (cole diretamente, SEM 'Bearer')")));
    }

    @Bean
    public OpenApiCustomizer sortTagsAndSchemas() {
        return openApi -> {
            // Ordena schemas
            if (openApi.getComponents() != null && openApi.getComponents().getSchemas() != null) {
                var sortedSchemas = new TreeMap<>(openApi.getComponents().getSchemas());
                openApi.getComponents().setSchemas(sortedSchemas);
            }

            // Ordena tags
            if (openApi.getTags() != null) {
                var sortedTags = openApi.getTags().stream()
                        .sorted((a, b) -> a.getName().compareToIgnoreCase(b.getName()))
                        .toList();
                openApi.setTags(sortedTags);
            }
        };
    }
}