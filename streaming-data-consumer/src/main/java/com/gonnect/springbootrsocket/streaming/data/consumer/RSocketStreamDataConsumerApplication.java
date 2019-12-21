package com.gonnect.springbootrsocket.streaming.data.consumer;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.*;
import org.reactivestreams.Publisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetSocketAddress;

import static io.rsocket.frame.decoder.PayloadDecoder.ZERO_COPY;
import static org.springframework.http.MediaType.TEXT_EVENT_STREAM_VALUE;
import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON;
import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@SpringBootApplication
public class RSocketStreamDataConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(RSocketStreamDataConsumerApplication.class, args);
    }

    @Bean
    @SneakyThrows
    RSocket rSocket() {
        return RSocketFactory.connect()
                .dataMimeType(APPLICATION_JSON_VALUE)
                .frameDecoder(ZERO_COPY)
                .transport(TcpClientTransport.create(new InetSocketAddress("127.0.0.1", 7000)))
                .start()
                .block();
    }

    @Bean
    RSocketRequester requester(RSocketStrategies strategies) {
        return RSocketRequester.wrap(
                rSocket(),
                APPLICATION_JSON,
                strategies
        );
    }
}


@Data
@NoArgsConstructor
@AllArgsConstructor
class ConsumerRequest {

    private String name;
}


@Data
@NoArgsConstructor
@AllArgsConstructor
class ConsumerAck {

    private String greeting;
}


@RequiredArgsConstructor
@RestController
class DataLake {

    private final RSocketRequester requester;

    @GetMapping("/datalake/{name}")
    public Publisher<ConsumerAck> greet(@PathVariable String name) {
        return requester
                .route("source")
                .data(new ConsumerRequest(name))
                .retrieveMono(ConsumerAck.class);
    }


    @GetMapping(value = "/datalake-stream/{name}", produces = TEXT_EVENT_STREAM_VALUE)
    public Publisher<ConsumerAck> greetStream(@PathVariable String name) {
        return requester
                .route("source-stream")
                .data(new ConsumerRequest(name))
                .retrieveFlux(ConsumerAck.class);
    }
}