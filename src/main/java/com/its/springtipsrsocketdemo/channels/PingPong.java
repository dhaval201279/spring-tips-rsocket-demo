package com.its.springtipsrsocketdemo.channels;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@SpringBootApplication
@Slf4j
public class PingPong {
    static String reply (String in) {
        log.info("Entering PingPong : reply");
        if (in.equalsIgnoreCase("ping") ) return "pong";
        if (in.equalsIgnoreCase("pong")) return "ping";
        throw new IllegalArgumentException("Incoming argument must be ping or pong");
    }

    public static void main(String [] args ) {
        log.info("Entering PingPong : main");
        SpringApplication.run(PingPong.class, args);
    }
}

@Component
@Slf4j
class Ping implements Ordered, ApplicationListener {

    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        log.info("Entering Ping : onApplicationEvent");

        RSocketFactory
            .connect()
            .transport(TcpClientTransport.create(7000))
            .start()
            .flatMapMany(
                    socket -> socket.requestChannel(
                            Flux
                                .interval(Duration.ofSeconds(1))
                                .map( i -> DefaultPayload.create("ping"))
                    )
                    .map(payload -> payload.getDataUtf8())
                    .doOnNext(str -> log.info("Received string {} in {} ", str, getClass().getName()))
                    .take(10)
                    .doFinally(signal -> socket.dispose())
            )
            .then()
            .block();
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}

@Component
@Slf4j
class Pong implements SocketAcceptor, Ordered, ApplicationListener {

    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        log.info("Entering Pong : onApplicationEvent");

        RSocketFactory
            .receive()
            .acceptor(this)
            .transport(TcpServerTransport.create(7000))
            .start()
            .subscribe();
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload connectionSetupPayload, RSocket rSocket) {
        AbstractRSocket rs = new AbstractRSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                return Flux
                        .from(payloads)
                        .map(Payload::getDataUtf8)
                        .doOnNext(str -> log.info("Received string {}, in class {} ", str, getClass().getName()))
                        .map(PingPong :: reply)
                        .map(DefaultPayload :: create)
                        ;
            }
        };
        return Mono.just(rs);
    }
}
