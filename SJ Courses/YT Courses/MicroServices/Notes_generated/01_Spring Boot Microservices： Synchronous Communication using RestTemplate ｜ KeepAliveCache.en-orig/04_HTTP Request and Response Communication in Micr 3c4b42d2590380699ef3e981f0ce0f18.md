# 04_HTTP Request and Response Communication in Microservices

## 1. Anatomy of HTTP Requests

In a microservices architecture, communication between decentralized applications is heavily reliant on the ***HTTP protocol***. 

> Every communication exchange starts with an HTTP request initiated by a client (upstream service) targeting a server (downstream service).
> 

### 1.1 The HTTP GET Request

An HTTP GET request is designed to retrieve resources from a target server without altering system state.

#### Core Components of a GET Request:

- **HTTP Method**: Specifies the action to be performed. In this case, `GET`.
- **Uniform Resource Identifier (URI)**: The path of the target resource on the server (e.g., `/products/101`).
- **Protocol Version**: The version of the HTTP protocol being used (e.g., `HTTP/1.1`).
- **Host Header**: Contains the destination domain/IP address and the target port number (e.g., `localhost:8082`). This represents the target host.
- **User-Agent Header**: Identifies the specific client library, browser, or tool initiating the request (e.g., `curl/7.68.0` or `PostmanRuntime/7.29.0`).
- **Accept Header**: Notifies the server of the media format the client expects in the return payload (e.g., `application/json`).

```
GET /products/101 HTTP/1.1
Host: localhost:8082
User-Agent: curl/7.68.0
Accept: application/json
```

---

### 1.2 The HTTP POST Request

An HTTP POST request is utilized to send data to the server to create a new resource or execute state-altering business logic.

#### Unique Attributes of a POST Request:

- **Content-Type Header**: Informs the server of the exact data serialization format used in the request body (e.g., `application/json`).
- **Content-Length Header**: Explicitly declares the size of the request body payload in bytes.
    - Explicitly declares the exact size of the payload in bytes so the server can detect payload truncation and verify full message transfer.
    - Clients (like web browsers or curl) calculate download or upload progress percentages by comparing the bytes received against `Content-Length`.
- **Request Body**: The serialized payload representing the resource data to be processed by the server.

```
POST /products HTTP/1.1
Host: localhost:8082
User-Agent: PostmanRuntime/7.29.0
Accept: application/json

Content-Type: application/json
Content-Length: 48

{
  "name": "Industrial Widget",
  "price": 49.99
}
```

---

## 2. Anatomy of HTTP Responses

When a server processes an incoming request, it returns an HTTP response containing execution metrics and the requested resource data.

### 2.1 Core Components of a Response:

- **Status Line**: Comprises the protocol version (e.g., `HTTP/1.1`) and the HTTP status code (e.g., `200 OK`, `201 Created`, or `404 Not Found`).
- **Content-Type**: Informs the client of the media format of the response body (e.g., `application/json`).
- **Content-Length**: Represents the exact size of the response payload in bytes.
- **Response Body**: The actual payload sent by the server in JSON, XML, or plain text.
- **Connection:** Dictates whether the network socket remains open or closes immediately after the current HTTP message finishes.
    - **Who uses it?** Both **Client** and **Server**.
    - **In Request or Response?** Both **Request** and **Response**.
- **Keep-Alive:** Provides specific **parameters and rules** for *how long* and *how much* the connection can be reused before being forcibly closed.
    - `timeout=X` → The maximum time (in seconds) the connection can sit idle before the server drops it.
    - `max=X` → The maximum number of sequential requests allowed over this single connection before it is closed.
    - **Who uses it?** Primarily sent by the **Server** (though clients can occasionally suggest values).
        - **In Request or Response?** Sent in the **Response**.

```
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 53

Connection: keep-alive
Keep-Alive: timeout=5, max=50

{
  "id": 101,
  "name": "Industrial Widget",
  "price": 49.99
}
```

---

## 3. TCP Connection Persistence: HTTP 1.0 vs. HTTP 1.1

A critical factor impacting synchronous microservice performance is how the underlying TCP connections are managed during successive HTTP operations.

### 3.1 HTTP 1.0 (Short-Lived Connections)

By default, HTTP 1.0 treats connections as non-persistent.

- The client establishes a TCP connection via a three-way handshake.
- The client sends a single request.
- The server processes the request, sends the response, and then ***immediately terminates the connection***.
- This is known as a `Connection: close` pattern. If a service needs to execute 10 sequential REST calls, it must pay the network latency penalty of 10 distinct TCP three-way handshakes and 10 four-way connection terminations.

```
[Client]                                    [Server]
   |                                           |
   | ------------ 3-Way Handshake -----------> | (TCP Established)
   | ------------ HTTP GET Request ----------> |
   | <----------- HTTP 200 Response ---------- |
   | <----------- TCP Connection Closed ------ | (Connection Terminated)
   v                                           v
```

### 3.2 HTTP 1.1 (Persistent Connections / Keep-Alive)

HTTP 1.1 introduced persistent connections as the default behavior. Under this model, the TCP connection remains open after the initial response is delivered, allowing subsequent HTTP requests to reuse the established socket.

#### Key Keep-Alive Headers:

- **Connection: keep-alive**: Indicates that the socket should remain open.
- **timeout**: Specifies the maximum duration (in seconds) that the connection can remain idle before the server terminates it (e.g., `timeout=5`).
- **max**: Declares the maximum number of requests that can be routed over this single TCP socket before it must be closed and re-established (e.g., `max=50`).

```
[Client]                                    [Server]
   |                                           |
   | ------------ 3-Way Handshake -----------> | (TCP Established)
   | ------------ HTTP GET Request (1) ------->|
   | <----------- HTTP 200 (Keep-Alive) ------ | (Socket Kept Open)
   |                                           |
   | ------------ HTTP GET Request (2) ------->| (Reuses Open Connection)
   | <----------- HTTP 200 (Keep-Alive) ------ |
   |                                           |
   |               ... Idle for 5s ...         |
   | <----------- 4-Way TCP Termination ------ | (Closed due to Timeout)
   v                                           v
```

---

## 4. Modern Spring Boot REST Clients and Connection Pooling

To implement proper HTTP 1.1 Keep-Alive connection reuse in Spring Boot, the ***default JDK-based HTTP clients are typically replaced with a pooled connection manager*** such as Apache HttpClient 5. This prevents socket exhaustion and minimizes latency.

### 4.1 Configuring a RestClient with Apache HttpClient 5 and Keep-Alive Strategy

```java
package com.example.client.config;

import org.apache.hc.client5.http.ConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.HeaderElement;
import org.apache.hc.core5.http.message.MessageSupport;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.util.Iterator;

@Configuration
public class HttpClientConfig {

    @Bean
    public PoolingHttpClientConnectionManager connectionManager() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(100);
        connectionManager.setDefaultMaxPerRoute(20);
        return connectionManager;
    }

    @Bean
    public ConnectionKeepAliveStrategy keepAliveStrategy() {
        return (response, context) -> {
            Iterator<HeaderElement> it = MessageSupport.iterate(response, "Keep-Alive");
            while (it.hasNext()) {
                HeaderElement he = it.next();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    try {
                        return TimeValue.ofSeconds(Long.parseLong(value));
                    } catch (NumberFormatException ignore) {
                    }
                }
            }
            // Fallback default: keep alive for 10 seconds if server doesn't send timeout
            return TimeValue.ofSeconds(10);
        };
    }

    @Bean
    public CloseableHttpClient apacheHttpClient(
            PoolingHttpClientConnectionManager connectionManager,
            ConnectionKeepAliveStrategy keepAliveStrategy) {

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.ofSeconds(3))
                .setResponseTimeout(Timeout.ofSeconds(5))
                .build();

        return HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setKeepAliveStrategy(keepAliveStrategy)
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    @Bean
    public RestClient restClient(CloseableHttpClient apacheHttpClient) {
        HttpComponentsClientHttpRequestFactory requestFactory =
                new HttpComponentsClientHttpRequestFactory(apacheHttpClient);

        return RestClient.builder()
                .requestFactory(requestFactory)
                .baseUrl("<http://localhost:8082>")
                .build();
    }
}
```

---

## 5. Programming Exercise

### 5.1 Problem Statement

In a microservices ecosystem, the **Order Service** needs to perform rapid, consecutive validation calls to the **Product Service**.

To optimize performance, configure a persistent HTTP connection pool in the **Order Service** using Spring Boot `RestClient` and Apache HttpClient 5. You must write a custom `ConnectionKeepAliveStrategy` that dynamically honors the downstream server's `Keep-Alive` timeout header. If the header is missing, the client must default the connection keep-alive duration to 15 seconds.

---

### 5.2 Context and Acceptance Criteria

1. **Product Service Mock Response**: Emulate the downstream service by sending a custom header `Keep-Alive: timeout=8, max=100` alongside response bodies.
2. **Order Service Keep-Alive Policy**: Establish a connection pool with a maximum of 50 total connections and 10 default connections per target route.
3. **Dynamic Inspection**: Create a custom keep-alive strategy in Java that safely parses the server's numeric `timeout` token.
4. **Error Handling**: Protect clients against connection timeouts or missing headers with safe fallback configurations.

---

### 5.3 Starter Code & Hints

Use this structure to implement the custom Keep-Alive parsing logic:

```java
import org.apache.hc.client5.http.ConnectionKeepAliveStrategy;
import org.apache.hc.core5.http.HeaderElement;
import org.apache.hc.core5.http.message.MessageSupport;
import org.apache.hc.core5.util.TimeValue;

public class CustomKeepAliveStrategy implements ConnectionKeepAliveStrategy {
    @Override
    public TimeValue getKeepAliveDuration(org.apache.hc.core5.http.HttpResponse response, org.apache.hc.core5.http.protocol.HttpContext context) {
        // Step 1: Iterate over Keep-Alive header elements
        // Step 2: Check for "timeout" parameter
        // Step 3: Parse and return TimeValue, or return a default of 15 seconds
        return TimeValue.ofSeconds(15);
    }
}
```

---

### 5.4 Detailed Solution

#### High-Performance RestClient Configuration

```java
package com.example.order.config;

import org.apache.hc.client5.http.ConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.HeaderElement;
import org.apache.hc.core5.http.message.MessageSupport;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.util.Iterator;

@Configuration
public class OrderClientConfig {

    @Bean
    public PoolingHttpClientConnectionManager connectionManager() {
        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
        manager.setMaxTotal(50);
        manager.setDefaultMaxPerRoute(10);
        return manager;
    }

    @Bean
    public ConnectionKeepAliveStrategy customKeepAliveStrategy() {
        return (response, context) -> {
            Iterator<HeaderElement> elementIterator = MessageSupport.iterate(response, "Keep-Alive");
            while (elementIterator.hasNext()) {
                HeaderElement element = elementIterator.next();
                String name = element.getName();
                String value = element.getValue();

                if (name != null && name.equalsIgnoreCase("timeout")) {
                    try {
                        return TimeValue.ofSeconds(Long.parseLong(value));
                    } catch (NumberFormatException ex) {
                        // Log warning or fallback
                    }
                }
            }
            // Explicit acceptance criteria default
            return TimeValue.ofSeconds(15);
        };
    }

    @Bean
    public CloseableHttpClient configuredHttpClient(
            PoolingHttpClientConnectionManager connectionManager,
            ConnectionKeepAliveStrategy customKeepAliveStrategy) {

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.ofSeconds(2))
                .setResponseTimeout(Timeout.ofSeconds(4))
                .build();

        return HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setKeepAliveStrategy(customKeepAliveStrategy)
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    @Bean
    public RestClient orderRestClient(CloseableHttpClient configuredHttpClient) {
        return RestClient.builder()
                .requestFactory(new HttpComponentsClientHttpRequestFactory(configuredHttpClient))
                .baseUrl("<http://localhost:8082>")
                .build();
    }
}
```

#### Order Validation Service Code

```java
package com.example.order.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import java.util.Map;

@Service
public class ProductValidationService {

    private final RestClient orderRestClient;

    public ProductValidationService(RestClient orderRestClient) {
        this.orderRestClient = orderRestClient;
    }

    public Map<?, ?> fetchProductDetails(Long productId) {
        return orderRestClient.get()
                .uri("/products/{id}", productId)
                .retrieve()
                .body(Map.class);
    }
}
```

---

### 5.5 Clarified Nuances and Edge Cases

1. **Dynamic vs. Default Keep-Alive**: If a downstream service runs in a containerized or serverless environment, it might completely omit `Keep-Alive` headers to scale down idle sockets aggressively. The custom strategy handles this by falling back to a safe 15-second value to maintain connection reuse locally without causing upstream exceptions.
2. **Max Total vs. Default Max Per Route**: `setMaxTotal(50)` dictates the entire client's socket pool. However, `setDefaultMaxPerRoute(10)` restricts the client to a maximum of 10 concurrent connections to any single hostname/port combination (e.g., `localhost:8082`). This prevents one malfunctioning downstream service from exhausting the entire connection pool.
3. **Header Parsing Robustness**: The use of `MessageSupport.iterate()` handles complex, comma-separated headers securely and protects against null or malformed timeout structures.