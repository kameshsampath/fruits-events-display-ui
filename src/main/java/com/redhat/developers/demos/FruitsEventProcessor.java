package com.redhat.developers.demos;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;

import io.cloudevents.v03.AttributesImpl;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.vertx.axle.core.eventbus.EventBus;
import io.vertx.axle.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * FruitsEventProcessor
 */
@ApplicationScoped
public class FruitsEventProcessor {

    Logger logger = Logger.getLogger(FruitsEventProcessor.class);
    
    @Incoming("fruit-events")
    @Outgoing("fruit-events-stream")
    @Broadcast
    public String processFruitEvent(String feJson ) {
        logger.info("SSE Data:" + feJson);
        return feJson;
    }
    
}