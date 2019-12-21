package com.gonnect.springbootrsocket.streaming.data.source.producer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

@SpringBootApplication
public class RSocketStreamingSource {

    public static void main(String[] args) {
        SpringApplication.run(RSocketStreamingSource.class, args);
    }

}


@Data
@NoArgsConstructor
@AllArgsConstructor
class SourceRegistration {

    private String name;
}


@Data
@NoArgsConstructor
@AllArgsConstructor
class SourceAck {

    private String greeting;
}

@Controller
class StreamingSource {


    @MessageMapping("source")
    Mono<SourceAck> source(SourceRegistration request) {
        return Mono.just(new SourceAck("Hello " + request.getName() + " @ " + Instant.now()));
    }

    @MessageMapping("source-stream")
    Flux<SourceAck> sourceStream(SourceRegistration request) {
        return Flux.fromStream(Stream.generate(
                () -> new SourceAck("Hello " + request.getName() + " @ " + Instant.now())
        )).delayElements(Duration.ofSeconds(1));
    }
}


