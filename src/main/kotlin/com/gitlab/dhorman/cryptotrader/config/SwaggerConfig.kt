package com.gitlab.dhorman.cryptotrader.config

import com.fasterxml.classmate.TypeResolver
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.Ordered.HIGHEST_PRECEDENCE
import org.springframework.stereotype.Component
import springfox.documentation.builders.ApiInfoBuilder
import springfox.documentation.builders.PathSelectors
import springfox.documentation.builders.RequestHandlerSelectors
import springfox.documentation.schema.AlternateTypeRule
import springfox.documentation.schema.AlternateTypeRuleConvention
import springfox.documentation.schema.AlternateTypeRules.newRule
import springfox.documentation.schema.WildcardType
import springfox.documentation.service.ApiInfo
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger.web.*
import springfox.documentation.swagger2.annotations.EnableSwagger2WebFlux

@Configuration
@EnableSwagger2WebFlux
class SwaggerConfig {
    @Bean
    fun api(): Docket {
        return Docket(DocumentationType.SWAGGER_2)
            .groupName("trader-api")
            .select()
            .apis(RequestHandlerSelectors.basePackage("com.gitlab.dhorman.cryptotrader.api"))
            .paths(PathSelectors.regex("/api/.*"))
            .build()
            .apiInfo(apiInfo())
            .useDefaultResponseMessages(false)
    }


    fun apiInfo(): ApiInfo {
        return ApiInfoBuilder()
            .title("Trader API")
            .version("1.0")
            .build()
    }

    @Bean
    fun uiConfig(): UiConfiguration {
        return UiConfigurationBuilder.builder()
            .deepLinking(true)
            .displayOperationId(false)
            .defaultModelsExpandDepth(0)
            .defaultModelExpandDepth(1)
            .defaultModelRendering(ModelRendering.EXAMPLE)
            .displayRequestDuration(true)
            .docExpansion(DocExpansion.LIST)
            .filter(false)
            .maxDisplayedTags(null)
            .operationsSorter(OperationsSorter.ALPHA)
            .showExtensions(false)
            .tagsSorter(TagsSorter.ALPHA)
            .validatorUrl("")
            .build()
    }

    // TODO: Review this workaround when new springfox library will be released
    @Component
    class VavrDefaultsConvention : AlternateTypeRuleConvention {
        @Autowired
        private lateinit var typeResolver: TypeResolver

        override fun rules(): List<AlternateTypeRule> {
            return listOf(
                newRule(
                    typeResolver.resolve(io.vavr.collection.List::class.java, WildcardType::class.java),
                    typeResolver.resolve(List::class.java, WildcardType::class.java)
                ),
                newRule(
                    typeResolver.resolve(io.vavr.collection.Set::class.java, WildcardType::class.java),
                    typeResolver.resolve(Set::class.java, WildcardType::class.java)
                ),
                newRule(
                    typeResolver.resolve(
                        io.vavr.collection.Map::class.java,
                        WildcardType::class.java,
                        WildcardType::class.java
                    ),
                    typeResolver.resolve(Map::class.java, WildcardType::class.java, WildcardType::class.java)
                )
            )
        }

        override fun getOrder(): Int {
            return HIGHEST_PRECEDENCE
        }
    }
}