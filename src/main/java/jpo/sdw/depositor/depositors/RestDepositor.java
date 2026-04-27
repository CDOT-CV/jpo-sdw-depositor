package jpo.sdw.depositor.depositors;

import java.net.URI;

import org.springframework.web.reactive.function.client.WebClient;

public abstract class RestDepositor<T extends Object> implements Depositor<T> {

   private final WebClient webClient;
   private final URI destination;

   public RestDepositor(WebClient webClient, URI destination) {
      this.webClient = webClient;
      this.destination = destination;
   }

   public WebClient getWebClient() {
      return webClient;
   }

   public URI getDestination() {
      return destination;
   }

}
