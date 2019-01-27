package com.its.springtipsrsocketdemo.requestresponse;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SpringBootApplication
@Slf4j
public class RequestResponse {
    public static void main(String args[]) {
        log.info("Entering and leaving RequestResponse : main");
        SpringApplication.run(RequestResponse.class, args);
    }
}

@Component
@Slf4j
class Producer implements Ordered, ApplicationListener<ApplicationReadyEvent> {
    @Override
    public int getOrder() {
        log.info("Entering and leaving Producer : getOrder");
        return Ordered.HIGHEST_PRECEDENCE;
    }

    Flux<String> notifications(String name) {
        return Flux
                .fromStream(
                    Stream
                        .generate(() -> "Hello " + name + " @  " + Instant.now().toString()))
                        .delayElements(Duration.ofSeconds(1)
                );
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        log.info("Entering Producer : onApplicationEvent");
        SocketAcceptor socketAcceptor = new SocketAcceptor() {
            @Override
            public Mono<RSocket> accept(ConnectionSetupPayload connectionSetupPayload, RSocket sender) {
                log.info("Instantiating AbstractRSocket and invoking Producer :  onApplicationEvent - accept");
                AbstractRSocket abstractRSocket = new AbstractRSocket() {
                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                        log.info("Entering Producer :  onApplicationEvent - accept - requestStream");
                        String name = payload.getDataUtf8();
                        return notifications(name)
                                    .map(DefaultPayload::create);
                        //return super.requestStream(payload);
                    }
                }; // TODO
                return Mono.just(abstractRSocket);
            }
        };

        TcpServerTransport transport = TcpServerTransport.create(7000);
        //WebsocketServerTransport websocketServerTransport = WebsocketServerTransport.create(7002);

        RSocketFactory
            .receive()
            .acceptor(socketAcceptor)
            .transport(transport)
            .start()
            .block()
            ;

    }
}

@Component
@Slf4j
class Consumer implements Ordered, ApplicationListener<ApplicationReadyEvent> {
    @Override
    public int getOrder() {
        log.info("Entering and leaving Consumer : getOrder");
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        log.info("Entering and leaving Consumer : onApplicationEvent");
        RSocketFactory
            .connect()
            .transport(TcpClientTransport.create(7000))
            .start()
            .flatMapMany(sender -> sender.requestStream(DefaultPayload.create("Spring Tips"))
                                         .map(payload -> payload.getDataUtf8())
                                         /*.doOnNext(s -> {
                                             log.info("Inside accept method of doOnNext of request stream. String accepted {} ", s);
                                         })*/
            )
            .subscribe(result -> log.info("Processing new result " + result));

    }
}