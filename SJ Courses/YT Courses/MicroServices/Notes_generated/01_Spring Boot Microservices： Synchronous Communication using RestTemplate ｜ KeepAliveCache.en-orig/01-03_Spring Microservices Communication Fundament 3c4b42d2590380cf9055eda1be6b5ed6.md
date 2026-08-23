# 01-03_Spring Microservices Communication: Fundamentals and Setup

## 1. Microservices Architecture and Setup

### 1.1 Service Orchestration Overview

In a typical microservices architecture, distinct business domains are decomposed into specialized, independent services running as separate processes. This guide focuses on two fundamental services:

- **Order Service**: Responsible for orchestrating purchase flows, configured to run on port `8081`.
- **Product Service**: Responsible for managing product catalogs and inventory details, configured to run on port `8082`.

### 1.2 Step-by-Step Initializer Setup

Both services are generated as barebones Spring Boot applications using Spring Initializr with the `spring-web` (Spring MVC) dependency. No Spring Cloud or service-discovery libraries are added at this preliminary stage to establish a clear baseline of direct service-to-service communication.

#### Order Service Configuration

The port for the Order Service is declared in its properties file:

- File: `order-service/src/main/resources/application.properties`

```
server.port=8081
```

#### Product Service Configuration

The port for the Product Service is declared in its properties file:

- File: `product-service/src/main/resources/application.properties`

```
server.port=8082
```

When both applications are launched, they execute independently on their respective ports (`8081` and `8082`), laying the groundwork for establishing cross-service integration.

---

## 2. Types of Communication in Spring Microservices

Communicating between decentralized services is a fundamental pillar of microservice design. There are two primary patterns:

1. **Synchronous Communication**
2. **Asynchronous Communication**

### 2.1 Synchronous (Blocking) Communication

In a synchronous communication flow, the client service initiates a request to a server service and halts its execution, waiting for the response before continuing further processing.

#### Characteristics of Synchronous Flows:

- **Blocking Nature**: The calling thread is blocked and remains idle while waiting for the target service to return the payload.
- **Tight Coupling**: The caller's latency is directly tied to the downstream service's response time.
    - **Latency** is the total time delay a client experiences from the moment they request until they receive the final response
- **Spring Mechanisms**: In Spring, synchronous REST calls can be made using `RestTemplate`, `RestClient`, or `FeignClient`.

```
+-----------------+                  +-------------------+
|  Order Service  |                  |  Product Service  |
|  (Client/Port   |                  |  (Server/Port     |
|     8081)       |                  |      8082)        |
+--------+--------+                  +---------+---------+
         |                                     |
         | --- (HTTP GET /product/{id}) -----> | [Process Request]
         |                                     |       |
         | =================[BLOCKED]==========|       | 
         |                                     |       |
         | <--- (200 OK JSON Product) -------- |<------+
         v                                     v
```

In a synchronous setup, when a user requests order creation:

1. **Order Service receives the request** and pauses its execution to fetch product/stock details.
2. **Order Service calls Product Service** via HTTP (`GET http://localhost:8082/products/{id}`).
3. **Order Service blocks (waits)** — its thread sits idle, holding the request open.
4. **Product Service processes the request** (runs DB lookup, logic, etc.) and returns a response back to Order Service.
5. **Order Service resumes execution** and returns the final response to the user.

$$
\text{Order Service Latency} = \text{Order Internal Processing} + \text{Product Service Response Time} + \text{Network Delay}
$$

### 2.2 Framework Approaches: With vs. Without Spring Cloud

When building communication pipelines, the choice of client libraries depends heavily on whether Spring Cloud is integrated into the architecture:

| Client Library | Environment Category | Integration Capabilities | Recommended Usage |
| --- | --- | --- | --- |
| **RestTemplate** | Without Spring Cloud | Legacy, template-driven synchronous HTTP requests. | Used for direct IP/port communication; acts as a base conceptual model. |
| **RestClient** | Without Spring Cloud | Modern, fluent, synchronous HTTP client (introduced in Spring 6 / Spring Boot 3). | Standard programmatic client for direct direct-service calls without service discovery. |
| **FeignClient** | With Spring Cloud | Declarative REST client with auto-integrated Spring Cloud features. | Recommended for Spring Cloud environments; auto-integrates load balancers, service discovery (Eureka), and observability. |

Understanding `RestTemplate` and `RestClient` is critical because they form the foundational baseline upon which declarative frameworks like `FeignClient` are designed internally.

---

## 3. Java and Spring Boot Implementation Examples

Below is a complete implementation showing how the **Product Service** (running on port `8082`) hosts a REST API, and how the **Order Service** (running on port `8081`) consumes it synchronously using standard Spring web clients.

### 3.1 Downstream Service: Product Service (Port 8082)

First, define the Product data transfer object (DTO) and the REST controller.

#### Product Record DTO

```java
package com.example.product.model;

public record ProductDto(Long id, String name, double price) {}
```

#### Product Controller

```java
package com.example.product.controller;

import com.example.product.model.ProductDto;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/products")
public class ProductController {

    @GetMapping("/{id}")
    public ResponseEntity<ProductDto> getProductById(@PathVariable Long id) {
        // Mock DB lookup
        ProductDto product = new ProductDto(id, "Industrial Widget", 49.99);
        return ResponseEntity.ok(product);
    }
}
```

---

### 3.2 Upstream Service: Order Service (Port 8081)

The Order Service consumes the Product Service API to validate product details when creating an order.

#### Product Client Configuration (using `RestClient` / `RestTemplate`)

```java
package com.example.order.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.RestClient;

@Configuration
public class ClientConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public RestClient restClient() {
        return RestClient.builder()
                .baseUrl("http://localhost:8082")
                .build();
    }
}
```

#### Order Service Domain Model

```java
package com.example.order.model;

public record OrderResponse(Long orderId, Long productId, String productName, double productPrice, int quantity) {}
```

#### Order Service Controller

This controller demonstrates how the Order Service synchronously blocks to fetch product information before returning the completed Order DTO.

```java
package com.example.order.controller;

import com.example.product.model.ProductDto; // Conceptual import of shared schema or duplicate local DTO
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final RestTemplate restTemplate;
    private final RestClient restClient;

    public OrderController(RestTemplate restTemplate, RestClient restClient) {
        this.restTemplate = restTemplate;
        this.restClient = restClient;
    }

    // Example 1: Synchronous Communication via classic RestTemplate
    @GetMapping("/via-template/{orderId}")
    public ResponseEntity<OrderResponse> getOrderWithRestTemplate(
            @PathVariable Long orderId,
            @RequestParam Long productId,
            @RequestParam int quantity) {

        String url = "<http://localhost:8082/products/>" + productId;

        // Blocking HTTP GET request
        ProductDto product = restTemplate.getForObject(url, ProductDto.class);

        if (product == null) {
            return ResponseEntity.notFound().build();
        }

        OrderResponse response = new OrderResponse(
                orderId,
                product.id(),
                product.name(),
                product.price(),
                quantity
        );
        return ResponseEntity.ok(response);
    }

    // Example 2: Synchronous Communication via modern RestClient
    @GetMapping("/via-client/{orderId}")
    public ResponseEntity<OrderResponse> getOrderWithRestClient(
            @PathVariable Long orderId,
            @RequestParam Long productId,
            @RequestParam int quantity) {

        // Blocking HTTP GET request using fluent API
        ProductDto product = restClient.get()
                .uri("/products/{id}", productId)
                .retrieve()
                .body(ProductDto.class);

        if (product == null) {
            return ResponseEntity.notFound().build();
        }

        OrderResponse response = new OrderResponse(
                orderId,
                product.id(),
                product.name(),
                product.price(),
                quantity
        );
        return ResponseEntity.ok(response);
    }
}
```

---

## 4. Programming Exercise

### 4.1 Problem Statement

You are required to implement a critical microservice integration. The **Order Service** (Port `8081`) must perform a validation check against the **Product Service** (Port `8082`) to verify if a specified product has enough stock inventory before allowing an order creation request to complete.

### 4.2 Context and Acceptance Criteria

1. The **Product Service** must expose a synchronous endpoint: `GET <http://localhost:8082/products/{id}/stock`> which returns a JSON representing inventory containing:
    - `productId` (Long)
    - `availableQuantity` (Integer)
2. The **Order Service** must expose an endpoint: `POST <http://localhost:8081/orders/validate`> taking a JSON payload:
    - `productId` (Long)
    - `requestedQuantity` (Integer)
3. The Order Service must execute a blocking, synchronous REST call using `RestClient` to get the current inventory stock level.
4. If the requested quantity is less than or equal to the available quantity, return a response of `200 OK` with a JSON payload containing `valid: true`.
5. If stock is insufficient, return `200 OK` with `valid: false` and a message stating `"Insufficient stock"`.
6. Handle edge cases where the Product Service is unreachable or returns a `404 Not Found`.

---

### 4.3 Starter Code & Hints

#### Hint 1: Product Service DTO Structure

```java
public record InventoryResponse(Long productId, Integer availableQuantity) {}
```

#### Hint 2: Catching HTTP Exceptions in Order Service

When calling external services synchronously, always handle connection failures and client-side or server-side HTTP errors:

```java
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
```

---

### 4.4 Detailed Solution

#### Product Service: Stock Controller Implementation (Port 8082)

```java
package com.example.product.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;
import java.util.HashMap;

@RestController
public class StockController {

    // Simple in-memory mock stock database
    private static final Map<Long, Integer> stockDatabase = new HashMap<>();

    static {
        stockDatabase.put(101L, 15);
        stockDatabase.put(102L, 0);
        stockDatabase.put(103L, 50);
    }

    @GetMapping("/products/{id}/stock")
    public ResponseEntity<?> getProductStock(@PathVariable Long id) {
        if (!stockDatabase.containsKey(id)) {
            return ResponseEntity.notFound().build();
        }

        Map<String, Object> response = new HashMap<>();
        response.put("productId", id);
        response.put("availableQuantity", stockDatabase.get(id));

        return ResponseEntity.ok(response);
    }
}
```

#### Order Service: Validation Service Implementation (Port 8081)

```java
package com.example.order.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import java.util.Map;
import java.util.HashMap;

@Service
public class OrderValidationService {

    private final RestClient restClient;

    // Direct initialization of RestClient pointed to Product Service Port 8082
    public OrderValidationService() {
        this.restClient = RestClient.builder()
                .baseUrl("<http://localhost:8082>")
                .build();
    }

    public Map<String, Object> validateOrderStock(Long productId, Integer requestedQuantity) {
        Map<String, Object> result = new HashMap<>();

        try {
            // Synchronous GET Request
            Map<?, ?> stockInfo = restClient.get()
                    .uri("/products/{id}/stock", productId)
                    .retrieve()
                    .body(Map.class);

            if (stockInfo == null || !stockInfo.containsKey("availableQuantity")) {
                result.put("valid", false);
                result.put("reason", "Could not parse stock information from Product Service");
                return result;
            }

            Integer availableQuantity = (Integer) stockInfo.get("availableQuantity");

            if (requestedQuantity <= availableQuantity) {
                result.put("valid", true);
                result.put("reason", "Stock is sufficient");
            } else {
                result.put("valid", false);
                result.put("reason", "Insufficient stock. Available: " + availableQuantity);
            }

        } catch (HttpClientErrorException.NotFound ex) {
            result.put("valid", false);
            result.put("reason", "Product with ID " + productId + " does not exist in inventory system");
        } catch (ResourceAccessException ex) {
            result.put("valid", false);
            result.put("reason", "Product Service (Port 8082) is currently down or unreachable");
        } catch (Exception ex) {
            result.put("valid", false);
            result.put("reason", "An unexpected error occurred: " + ex.getMessage());
        }

        return result;
    }
}
```

#### Order Service: Validation Controller (Port 8081)

```java
package com.example.order.controller;

import com.example.order.service.OrderValidationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderValidationService validationService;

    public OrderController(OrderValidationService validationService) {
        this.validationService = validationService;
    }

    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateOrder(@RequestBody Map<String, Object> requestPayload) {
        Long productId = Long.valueOf(requestPayload.get("productId").toString());
        Integer requestedQuantity = Integer.valueOf(requestPayload.get("requestedQuantity").toString());

        Map<String, Object> validationResponse = validationService.validateOrderStock(productId, requestedQuantity);
        return ResponseEntity.ok(validationResponse);
    }
}
```

---

### 4.5 Clarified Nuances and Edge Cases

1. **Thread Blocking**: During the call `restClient.get().uri(...).retrieve().body(...)`, the executing thread inside Order Service goes to sleep. It will wake up only when the connection times out or Product Service responds with data. Under high-traffic workloads, this blocking synchronous pattern can quickly exhaust the Tomcat thread pool.
2. **Unavailability Handling**: The `ResourceAccessException` catch block handles cases where the target microservice is offline, preventing a generic stack trace leak to the calling system.
3. **Missing Resources**: When a requested product is not configured in the target system, it returns an HTTP `404 Not Found`. `HttpClientErrorException.NotFound` handles this cleanly and returns a structured validation failure.