package com.shaili.kafka_order_system.loadtest;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import java.time.Duration;
import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

public class OrderSimulation extends Simulation {

    HttpProtocolBuilder httpProtocol = http
            .baseUrl("http://localhost:8080");

    ScenarioBuilder scn = scenario("Order placement load test")
            .exec(
                    http("place_order")
                            .post("/orders?item=LoadTestItem")
                            .check(status().is(200)));

    {
        setUp(
                scn.injectOpen(
                        rampUsersPerSec(5).to(50).during(Duration.ofSeconds(30)),
                        constantUsersPerSec(50).during(Duration.ofSeconds(30))))
                .protocols(httpProtocol);
    }
}