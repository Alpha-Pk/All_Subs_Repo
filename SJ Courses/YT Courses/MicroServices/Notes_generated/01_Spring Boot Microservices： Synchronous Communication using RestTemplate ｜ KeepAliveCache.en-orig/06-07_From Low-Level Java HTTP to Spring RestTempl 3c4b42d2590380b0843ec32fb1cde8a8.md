# 06-07_From Low-Level Java HTTP to Spring RestTemplate

## 1. Architectural Constraints and Disadvantages of Plain Java HTTP

Relying on standard JDK libraries (such as `java.net.HttpURLConnection`) to facilitate synchronous communication between microservices introduces substantial technical debt, operational overhead, and architectural bottlenecks.

### 1.1 Excessive Boilerplate and Lifecycle Overhead

Using plain Java requires developers to write redundant boilerplate code to manage every phase of the connection lifecycle manually. This includes:

- **Socket Establishment**: Open a TCP connection explicitly.
- **Protocol Configuration**: Manually define request methods, headers, and payload attributes.
- **I/O Stream Management**: Acquire, read, and write bytes or characters from the input and output streams.
- **Resource Deallocation**: Explicitly close streams and disconnect sockets in structured `finally` blocks to prevent severe OS-level file descriptor leaks.

### 1.2 Manual Response Deserialization

Plain Java provides no native support for data-binding. Developers must write parsing logic to read raw input streams line-by-line using constructs like `BufferedReader` and `StringBuilder`.

If a downstream microservice responds with a JSON payload, mapping it to a strongly-typed Java object requires importing third-party libraries (such as Jackson or Gson) and manually executing the deserialization lifecycle, increasing the potential for parsing errors and null-pointer exceptions.

### 1.3 Lack of Enterprise-Grade Features

Low-level classes provide minimal or highly complex paths to implement advanced networking features:

- **Connection Pooling**: Without connection pooling, a new TCP handshake must be executed for every single request, degrading application throughput.
- **Interceptors and Filters**: Injecting cross-cutting concerns (e.g., logging headers, tracking tokens, security authorization) requires wrapping every outbound call manually.
- **Resiliency Patterns**: Implementing retries, circuit breakers, and customized error boundaries is cumbersome and error-prone when written against bare streams.

---

## 2. Spring RestTemplate: Foundational Concepts and Configuration

Spring Framework abstracts low-level I/O complexities via `RestTemplate`. While considered a legacy option in Spring 6 and Spring Boot 3 due to the introduction of the non-blocking `WebClient` and the synchronous, fluent `RestClient`, `RestTemplate` remains a highly prevalent tool in enterprise applications.

### 2.1 Basic Beans Configuration

To instantiate and leverage `RestTemplate` within a Spring container, you must declare it as a Spring-managed Bean within a configuration class.

```java
package com.example.communication.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class HttpClientConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
```

### 2.2 Synchronous Consumption with `getForObject()`

`RestTemplate` provides convenient templated methods that map to HTTP verbs. The `getForObject()` method performs an HTTP GET request and automatically marshals the returned JSON into a Java Object or a String.

```java
package com.example.communication.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class SimpleProductClient {

    private final RestTemplate restTemplate;

    @Autowired
    public SimpleProductClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public String fetchProductRawResponse(Long productId) {
        String targetUrl = "http://localhost:8082/products/" + productId;
        // Performs a synchronous GET, automatically converts the JSON stream to String
        return restTemplate.getForObject(targetUrl, String.class);
    }
}
```

By specifying `String.class` or a custom DTO, the underlying Jackson message converters

1. Read the HTTP input stream,
2. Deserialize the payload, and 
3. Close the response streams automatically behind the scenes.

---

## 3. Advanced Configuration: Timeout Management

In production, microservices must never rely indefinitely on default timeout configurations. If a downstream service hangs, the thread executing the request will block forever, rapidly exhausting the Tomcat thread pool.

### 3.1 Client HTTP Request Factories

`RestTemplate` does not implement HTTP transport protocols itself; it delegates transport tasks to an underlying library. By default, it uses `SimpleClientHttpRequestFactory`, which relies on standard JDK `HttpURLConnection`.

To configure connection and read timeouts, instantiate the factory, configure its properties, and inject it into the `RestTemplate` builder.

```java
package com.example.communication.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class TimeoutClientConfig {

    @Bean
    public RestTemplate restTemplateWithTimeouts() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();

        // Timeouts are specified in milliseconds
        factory.setConnectTimeout(3000); // Wait up to 3 seconds to establish a TCP socket connection
        factory.setReadTimeout(5000);    // Wait up to 5 seconds for data packets after establishing connection

        return new RestTemplate(factory);
    }
}
```

### 3.2 Internals of the Default Request Factory

The `SimpleClientHttpRequestFactory` acts as a bridge between Spring’s abstraction model and the JVM’s networking stack. It produces `ClientHttpRequest` objects that map connection configurations directly to `HttpURLConnection` calls. When timeouts are set, it applies them during socket instantiation by invoking:

- `java.net.HttpURLConnection.setConnectTimeout(int timeout)`
- `java.net.HttpURLConnection.setReadTimeout(int timeout)`

---

## 4. End-to-End Code Examples

Below is a complete, production-ready Spring Boot integration showing a Product Service API on Port 8082 and an Order Service on Port 8081 consuming it via a configured `RestTemplate`.

### 4.1 Product Service (Port 8082)

#### Product Model

```java
package com.example.product.model;

public record Product(Long id, String name, double price, String sku) {}
```

#### Product Controller

```java
package com.example.product.controller;

import com.example.product.model.Product;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/products")
public class ProductController {

    @GetMapping("/{id}")
    public ResponseEntity<Product> getProduct(@PathVariable Long id) {
        // Return simulated product database record
        if (id == 999) {
            return ResponseEntity.notFound().build();
        }
        Product product = new Product(id, "Advanced Processor Core", 299.99, "PRD-CORE-001");
        return ResponseEntity.ok(product);
    }
}
```

### 4.2 Order Service (Port 8081)

#### Local Product Representation (DTO)

```java
package com.example.order.dto;

public record ProductDto(Long id, String name, double price, String sku) {}
```

#### Client Configuration

```java
package com.example.order.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class OrderClientConfig {

    @Bean
    public RestTemplate productRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2500);
        factory.setReadTimeout(4000);
        return new RestTemplate(factory);
    }
}
```

#### Controller Demonstrating Integration

```java
package com.example.order.controller;

import com.example.order.dto.ProductDto;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/orders")
public class OrderIntegrationController {

    private final RestTemplate restTemplate;

    public OrderIntegrationController(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @GetMapping("/validate-product/{productId}")
    public ResponseEntity<Map<String, Object>> validateProductDetails(@PathVariable Long productId) {
        String targetUrl = "<http://localhost:8082/products/>" + productId;
        Map<String, Object> response = new HashMap<>();

        try {
            // Synchronously requests and maps JSON payload
            ProductDto product = restTemplate.getForObject(targetUrl, ProductDto.class);

            if (product != null) {
                response.put("status", "SUCCESS");
                response.put("productName", product.name());
                response.put("price", product.price());
                response.put("message", "Product verified successfully");
                return ResponseEntity.ok(response);
            } else {
                response.put("status", "FAILED");
                response.put("message", "Received empty response from Product Service");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
            }

        } catch (HttpClientErrorException.NotFound ex) {
            response.put("status", "INVALID_PRODUCT");
            response.put("message", "Product does not exist in downstream inventory");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        } catch (ResourceAccessException ex) {
            response.put("status", "UNREACHABLE");
            response.put("message", "Downstream product service timed out or is down");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
        } catch (Exception ex) {
            response.put("status", "ERROR");
            response.put("message", "Internal integration exception: " + ex.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
}
```

---

## 5. Programming Exercises

To build proficiency in designing robust inter-service communication layers, complete the following hands-on programming challenges.

### 5.1 Exercise: RestTemplate Timeout Configuration and Custom Error Boundary

#### Problem Statement

In an e-commerce platform, the **Order Service** requires verifying if a **Coupon Code** is valid before processing payment. The validation is hosted by a legacy, unreliable **Promotion Service** running on port `8083`.

Implement an integration endpoint inside the Order Service that checks a coupon using `RestTemplate`. Because the promotion service is highly unstable and prone to sporadic slowdowns, you must enforce a maximum connection timeout of `1500 milliseconds` and a read timeout of `2000 milliseconds`.

#### Acceptance Criteria

1. The Order Service must expose a POST endpoint at `POST <http://localhost:8081/orders/promotions/apply`>.
2. The endpoint receives a JSON body representing the discount check request:
    - `couponCode` (String)
    - `cartTotal` (double)
3. The Order Service must call the legacy promotion endpoint: `POST <http://localhost:8083/promotions/validate`> using `RestTemplate`.
4. If the legacy service successfully validates the coupon, it returns `200 OK` with JSON containing `discountAmount` (double) and `isValid` (boolean). Apply this discount to the payload and return the calculated result to the caller.
5. If the promotion service takes longer than `2000 milliseconds` to process the request, a socket timeout must occur.
6. If a read or connection timeout occurs, or if the server is unreachable, the Order Service must catch the exception and fallback gracefully to applying a `0.00` discount, permitting the order transaction to proceed without failing.

#### Starter Code Structures

#### Request DTO

```java
package com.example.order.exercise.dto;

public record CouponRequest(String couponCode, double cartTotal) {}
```

#### Response DTO

```java
package com.example.order.exercise.dto;

public record CouponResponse(double discountAmount, boolean isValid) {}
```

#### Processed Order Result

```java
package com.example.order.exercise.dto;

public record AppliedDiscountResult(double finalPrice, double discountApplied, String message) {}
```

---

### 5.2 Detailed Implementation Solution

#### Mock Promotion Service Controller (Port 8083)

This mock simulates the slow promotional processing times.

```java
package com.example.promotion.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;
import java.util.HashMap;

@RestController
public class PromotionMockController {

    @PostMapping("/promotions/validate")
    public ResponseEntity<Map<String, Object>> validateCoupon(@RequestBody Map<String, Object> payload) throws InterruptedException {
        String couponCode = (String) payload.get("couponCode");
        double cartTotal = (double) payload.get("cartTotal");

        // Simulating systemic instability
        if ("SLOW_CODE".equalsIgnoreCase(couponCode)) {
            Thread.sleep(5000); // Exceeds the 2000ms read timeout threshold
        }

        Map<String, Object> response = new HashMap<>();
        if ("WINTER30".equalsIgnoreCase(couponCode)) {
            response.put("discountAmount", cartTotal * 0.3);
            response.put("isValid", true);
        } else {
            response.put("discountAmount", 0.0);
            response.put("isValid", false);
        }

        return ResponseEntity.ok(response);
    }
}
```

#### Order Service Config with Custom Request Factory (Port 8081)

```java
package com.example.order.exercise.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class ExerciseClientConfig {

    @Bean(name = "promoRestTemplate")
    public RestTemplate promoRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();

        // Strict production timeout limits
        factory.setConnectTimeout(1500);
        factory.setReadTimeout(2000);

        return new RestTemplate(factory);
    }
}
```

#### Order Service Integration Controller & Fallback Logic (Port 8081)

```java
package com.example.order.exercise.controller;

import com.example.order.exercise.dto.AppliedDiscountResult;
import com.example.order.exercise.dto.CouponRequest;
import com.example.order.exercise.dto.CouponResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/orders/promotions")
public class PromotionClientController {

    private final RestTemplate restTemplate;

    public PromotionClientController(@Qualifier("promoRestTemplate") RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @PostMapping("/apply")
    public ResponseEntity<AppliedDiscountResult> applyCouponCode(@RequestBody CouponRequest request) {
        String targetUrl = "<http://localhost:8083/promotions/validate>";

        try {
            // Post payload synchronously to the promotion service
            CouponResponse response = restTemplate.postForObject(targetUrl, request, CouponResponse.class);

            if (response != null && response.isValid()) {
                double finalPrice = request.cartTotal() - response.discountAmount();
                return ResponseEntity.ok(new AppliedDiscountResult(
                        finalPrice,
                        response.discountAmount(),
                        "Promotion discount applied successfully."
                ));
            } else {
                return ResponseEntity.ok(new AppliedDiscountResult(
                        request.cartTotal(),
                        0.0,
                        "Promotion code is expired or invalid."
                ));
            }

        } catch (RestClientException ex) {
            // Graceful fallback execution path on Client Timeout, 5xx error, or Service Down state
            double fallbackPrice = request.cartTotal();
            String fallbackMessage = "Promotion validation offline. Proceeding with regular total. Error: " + ex.getMessage();

            return ResponseEntity.ok(new AppliedDiscountResult(
                    fallbackPrice,
                    0.0,
                    fallbackMessage
            ));
        }
    }
}
```

---

### 5.3 Technical Insights and Edge-Case Analysis

1. **Thread Pool Resource Depletion Prevention**: By placing a `2000ms` limit on read execution blocks, the Order Service guarantees that threads waiting for `/promotions/validate` are reclaimed within a deterministic timeframe rather than exhausting the underlying container's connection pool.
2. **Fallback Patterns**: Throwing an application-level HTTP error response (like HTTP `500` or `503`) when a non-critical integration fails can severely ruin user experience. Utilizing a `try-catch` block trapping `RestClientException` lets the application isolate downstream integration failures and execute a fallback strategy (returning 0% discount) to keep the primary cart checkout flow online.
3. **Automatic JSON Mapping**: The `postForObject()` API automatically looks at the class parameters of the payload (`CouponRequest`) and the response class mapping (`CouponResponse`). Internally, it leverages standard Jackson HTTP converters, eliminating manual socket configuration, parameter outputting, and stream readers.