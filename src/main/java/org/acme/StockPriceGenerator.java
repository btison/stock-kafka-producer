package org.acme;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

/**
 * A bean producing random prices every 5 seconds.
 * The prices are written to a Kafka topic (prices). The Kafka configuration is specified in the application configuration.
 */
@ApplicationScoped
public class StockPriceGenerator {

    private Random random = new Random();

    private Map<String, Double> stocks = new HashMap<>();

    @PostConstruct
    public void init() {
        stocks.put("META", 180.00);
        stocks.put("IBM", 133.00);
        stocks.put("MSFT", 257.00);
        stocks.put("GOOGL", 2000.00);
        stocks.put("AAPL", 140.00);
        stocks.put("ORCL", 70.00);
        stocks.put("VNW", 116.00);
        stocks.put("HPQ", 34.00);
        stocks.put("DXC", 28.99);
        stocks.put("SAP", 91.00);
    }


    @Outgoing("stocks")
    public Multi<String> generate() {
        return Multi.createFrom().ticks().every(Duration.ofSeconds(5))
                .onOverflow().drop()
                .map(tick -> {
                    int k = random.nextInt(stocks.keySet().size());
                    String stock = List.copyOf(stocks.keySet()).get(k);
                    int v = random.nextInt(7);
                    double p = stocks.get(stock);
                    BigDecimal bd = BigDecimal.valueOf(p + (p * (((double)(v - 3)) / 100))).setScale(2, RoundingMode.HALF_UP);
                    p = bd.doubleValue();
                    stocks.put(stock, p);
                    return new JsonObject().put("stock", stock).put("price", p).toString();
                });
    }

}
