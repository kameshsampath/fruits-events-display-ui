package com.redhat.developers.demos;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jboss.logging.Logger;
import org.reactivestreams.Publisher;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.http.Unmarshallers;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.vertx.core.json.JsonObject;

@Path("/")
public class FruitsResource {

    Logger logger = Logger.getLogger(FruitsResource.class);

    @Inject
    @Channel("fruit-events")
    Emitter<String> fruitEventsEmitter;

    @Inject
    @Channel("fruit-events-stream")
    Publisher<String> fruitEvents;

    
    @GET
    @Path("/fruits")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public Publisher<String> fruitProcessor() {
        return fruitEvents;
    }

    @POST
    @Consumes("application/cloudevents+json")
    @Produces(MediaType.APPLICATION_JSON)
    public Response fruitsHandler(String cloudEventPayload) {
        Map<String, Object> httpHeaders = new HashMap<>();

        CloudEvent<AttributesImpl, Map> event = Unmarshallers.structured(Map.class)
        .withHeaders(() -> httpHeaders)
        .withPayload(() -> cloudEventPayload)
        .unmarshal();

        AttributesImpl attrs = event.getAttributes();
        logger.info("Attributes:" + attrs);
        Optional<Map> data = event.getData();
        logger.info("Data:" + data);
        logger.info("Extensions:" + event.getExtensions());

        JsonObject feJson = new JsonObject().put("id", attrs.getId()).put("type", attrs.getType());

        Optional<ZonedDateTime> eventTS = attrs.getTime();

        if (eventTS.isPresent()) {
            LocalTime lt = eventTS.get().toLocalTime();
            feJson.put("time", lt.toString());
        }

        if (data.isPresent()) {
            Map data2 = data.get();
            feJson.put("name", data2.get("name"));
            feJson.put("sugarLevel", ((Map) data2.get("nutritions")).get("sugar"));
        }

        fruitEventsEmitter.send(feJson.encodePrettily());

        return Response.ok("{\"ok\": \"TRUE\"}").build();
    }

   
}