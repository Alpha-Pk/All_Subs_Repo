# 09_Spring RestTemplate Communication Methods and Low-Level Internals

## 1. Core Request-Response Communication Methods

`RestTemplate` provides a comprehensive suite of helper methods that target standard HTTP operations. These methods range from extracting only the unmarshalled response body to returning full status metadata and header maps.

### 1.1 HTTP GET Operations: `getForObject` vs. `getForEntity`

When consuming HTTP GET endpoints, `RestTemplate` separates simple data extraction from complete response inspection through two primary methods:

- **`getForObject`**: Extracts the HTTP response body and automatically deserializes it into the requested Java class type. All response headers and HTTP status codes are discarded. It is ideal when only the payload is required.
- **`getForEntity`**: Wraps the deserialized payload alongside HTTP headers and status codes inside a `ResponseEntity` wrapper. It is useful when validation of response metadata (such as custom headers or redirect statuses) is required.

#### Programmatic Comparison

```java
package com.example.communication.service;

import com.example.communication.model.ProductDto;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class ProductRetrievalService {

    private final RestTemplate restTemplate;
    private static final String BASE_URL = "<http://localhost:8082/products/>";

    public ProductRetrievalService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * Retrieves only the response body payload.
     */
    public ProductDto getProductBodyOnly(Long productId) {
        String url = BASE_URL + productId;
        // Returns the response body directly, discarding headers and status codes
        return restTemplate.getForObject(url, ProductDto.class);
    }

    /**
     * Retrieves the complete ResponseEntity including headers and status.
     */
    public ResponseEntity<ProductDto> getProductCompleteEntity(Long productId) {
        String url = BASE_URL + productId;
        // Returns the full ResponseEntity wrapper
        ResponseEntity<ProductDto> responseEntity = restTemplate.getForEntity(url, ProductDto.class);

        // Metadata inspection is now possible
        int statusCode = responseEntity.getStatusCode().value();
        org.springframework.http.HttpHeaders headers = responseEntity.getHeaders();
        ProductDto body = responseEntity.getBody();

        return responseEntity;
    }
}
```

---

### 1.2 HTTP POST Operations: `postForObject` vs. `postForEntity`

HTTP POST operations send a client payload to create resources downstream. Similar to GET, `RestTemplate` offers object-only and full-entity options:

- **`postForObject`**: Submits the request payload, performs automated content-negotiation to map the request body, and returns only the deserialized response body payload.
- **`postForEntity`**: Submits the request payload and returns the response body wrapped in a `ResponseEntity`, giving the client full access to headers (e.g., the `Location` header indicating the URI of the newly created resource) and status codes (e.g., `201 Created`).

#### Programmatic Comparison

```java
package com.example.communication.service;

import com.example.communication.model.ProductDto;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class ProductCreationService {

    private final RestTemplate restTemplate;
    private static final String BASE_URL = "<http://localhost:8082/products>";

    public ProductCreationService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * Creates a product and returns only the body of the response.
     */
    public ProductDto createProductAndGetBody(ProductDto newProduct) {
        // Parameters: (URL, Request Payload, Deserialization Target Class)
        return restTemplate.postForObject(BASE_URL, newProduct, ProductDto.class);
    }

    /**
     * Creates a product and returns the complete ResponseEntity wrapper.
     */
    public ResponseEntity<ProductDto> createProductAndGetEntity(ProductDto newProduct) {
        // Parameters: (URL, Request Payload, Deserialization Target Class)
        ResponseEntity<ProductDto> response = restTemplate.postForEntity(BASE_URL, newProduct, ProductDto.class);

        System.out.println("HTTP Status Code: " + response.getStatusCode().value());
        System.out.println("Response Headers: " + response.getHeaders().toString());

        return response;
    }
}
```

---

### 1.3 HTTP PUT and DELETE Operations

Unlike GET and POST, HTTP PUT and DELETE verbs typically do not expect a response body. Accordingly, the standard helper methods in `RestTemplate` return `void` and focus strictly on executing the HTTP command:

- **`put`**: Executes an HTTP PUT request targeting the resource URI. It takes the target URI and the updated payload as arguments, performing serialization on the body without returning any content.
- **`delete`**: Executes an HTTP DELETE request targeting the specified URI. It accepts only the URI (including any path parameters) and returns nothing.

#### Programmatic Examples

```java
package com.example.communication.service;

import com.example.communication.model.ProductDto;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class ProductMaintenanceService {

    private final RestTemplate restTemplate;
    private static final String BASE_URL = "<http://localhost:8082/products/{id}>";

    public ProductMaintenanceService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * Updates an existing resource using HTTP PUT.
     */
    public void updateProduct(Long productId, ProductDto updatedProduct) {
        Map<String, Object> uriVariables = new HashMap<>();
        uriVariables.put("id", productId);

        // Parameters: (URI Template, Request Payload, URI Variables)
        restTemplate.put(BASE_URL, updatedProduct, uriVariables);
    }

    /**
     * Deletes a resource using HTTP DELETE.
     */
    public void removeProduct(Long productId) {
        Map<String, Object> uriVariables = new HashMap<>();
        uriVariables.put("id", productId);

        // Parameters: (URI Template, URI Variables)
        restTemplate.delete(BASE_URL, uriVariables);
    }
}
```

---

## 2. Dynamic Request Configuration via `exchange`

When microservice communication requires structural customisation that exceeds the capabilities of standard helper methods, the `exchange` method serves as a highly flexible API.

### 2.1 Core Architectural Capabilities

- **Custom HTTP Verbs**: Executes standard verbs (GET, POST, PUT, DELETE) as well as advanced methods (PATCH, OPTIONS, HEAD).
- **Custom Request Headers**: Allows injection of essential transaction metadata, authorization tokens (e.g., Bearer tokens), custom tracing IDs (e.g., Zipkin headers), and custom `Content-Type` parameters.
- **Preserved Message Conversion**: While providing deep control over the request headers and bodies, it relies completely on Spring’s underlying `HttpMessageConverter` infrastructure (like Jackson) to handle automatic JSON serialization and deserialization.

### 2.2 Constructing `HttpEntity` and Invoking `exchange`

To leverage `exchange`, we wrap both the client payload (the body) and the metadata (`HttpHeaders`) into a single `HttpEntity` container.

```java
package com.example.communication.service;

import com.example.communication.model.ProductDto;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class AdvancedProductService {

    private final RestTemplate restTemplate;

    public AdvancedProductService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public ResponseEntity<ProductDto> createProductWithHeaders(ProductDto product, String securityToken) {
        String url = "<http://localhost:8082/products>";

        // Step 1: Instantiation and population of custom headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer " + securityToken);
        headers.set("X-Correlation-ID", "f28b3a-9c2d-45af-8fca");

        // Step 2: Encapsulation of payload and headers inside HttpEntity
        HttpEntity<ProductDto> requestEntity = new HttpEntity<>(product, headers);

        // Step 3: Invoke the target microservice via exchange
        // Parameters: (URL, HttpMethod, HttpEntity request, Target Response Class)
        ResponseEntity<ProductDto> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                requestEntity,
                ProductDto.class
        );

        return response;
    }
}
```

---

## 3. Low-Level Execution Control via `execute`

The `execute` method represents the foundational, lowest-level core entry point of `RestTemplate`. In fact, all high-level convenience methods (`getForObject`, `postForEntity`, `exchange`, `put`, `delete`) delegate their work internally to `execute` to orchestrate network connections.

### 3.1 Architectural Philosophy

When calling `execute`, Spring relinquishes high-level convenience and automation. The client is fully responsible for manually configuring the HTTP request headers and body, converting objects to and from byte streams, and parsing server responses. It mirrors plain Java connection workflows while retaining the structural connection management and socket lifecycle wrapper of the Spring framework.

### 3.2 Request Callback and Response Extractor

The execution pipeline is driven by two functional interfaces, typically implemented using lambda expressions:

1. **`RequestCallback`**:
    - Exposes the `ClientHttpRequest` object.
    - Provides access to raw request headers and the active output stream via `request.getBody()`.
    - Requires manual serialization: Objects must be manually mapped to bytes (e.g., using Jackson’s `ObjectMapper`) and written directly to the request stream.
2. **`ResponseExtractor<T>`**:
    - Exposes the `ClientHttpResponse` object.
    - Provides raw access to the response status code, response headers, and the response data stream via `response.getBody()`.
    - Requires manual deserialization: Bytes must be read from the stream and manually mapped back to Java object domains.

### 3.3 Internal Thread Execution Lifecycle

When `execute` is invoked, it coordinates the connection through the following discrete sequence:

```
+--------------------------------------------------------------------------------------+
|                                    RestTemplate.execute()                            |
+--------------------------------------------------------------------------------------+
                                             |
                                             v
               +-----------------------------------------------------------+
               | 1. Query ClientHttpRequestFactory to initiate connection  |
               +-----------------------------------------------------------+
                                             |
                                             v
               +------------------------------------------------------------+
               | 2. Resolve target URL and establish physical TCP handshake |
               +------------------------------------------------------------+
                                             |
                                             v
               +-----------------------------------------------------------+
               | 3. Invoke RequestCallback lambda:                         |
               |    - Inject ClientHttpRequest                             |
               |    - Set manual HTTP Headers                              |
               |    - Serialize Java object -> byte[]                      |
               |    - Write bytes directly to ClientHttpRequest.getBody()  |
               +-----------------------------------------------------------+
                                             |
                                             v
               +-----------------------------------------------------------+
               | 4. Flush Request stream and transmit payload to target    |
               +-----------------------------------------------------------+
                                             |
                                             v
               +-----------------------------------------------------------+
               | 5. Wait for downstream processing (BLOCKING THREAD)       |
               +-----------------------------------------------------------+
                                             |
                                             v
               +-----------------------------------------------------------+
               | 6. Receive status header, parse HTTP response code        |
               +-----------------------------------------------------------+
                                             |
                                             v
               +-----------------------------------------------------------+
               | 7. Invoke ResponseExtractor lambda:                       |
               |    - Inject ClientHttpResponse                            |
               |    - Stream-read bytes from ClientHttpResponse.getBody()  |
               |    - Convert/Deserialize stream to output domain object   |
               +-----------------------------------------------------------+
                                             |
                                             v
               +-----------------------------------------------------------+
               | 8. Close response stream, release socket back to Pool     |
               +-----------------------------------------------------------+
```

---

### 3.4 Low-Level `execute` Implementation Example

The following class demonstrates a manual HTTP POST transaction using `execute`, complete with Jackson object-to-byte serialization and custom stream copying:

```java
package com.example.communication.service;

import com.example.communication.model.ProductDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestTemplate;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Service
public class ManualExecutionService {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public ManualExecutionService(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public ProductDto executeManualPost(ProductDto productToCreate) {
        String url = "<http://localhost:8082/products>";

        // Call execute(URL, HttpMethod, RequestCallback, ResponseExtractor, URI Variables)
        return restTemplate.execute(
                url,
                org.springframework.http.HttpMethod.POST,
                // RequestCallback implementation
                request -> {
                    // 1. Manually add headers to the HTTP client request
                    request.getHeaders().setContentType(MediaType.APPLICATION_JSON);
                    request.getHeaders().set("X-Manual-Client", "Spring-RestTemplate-Core");

                    // 2. Manually serialize ProductDto to JSON byte array
                    byte[] serializedPayload = objectMapper.writeValueAsBytes(productToCreate);

                    // 3. Write bytes directly into the request output stream
                    request.getBody().write(serializedPayload);
                },
                // ResponseExtractor implementation
                response -> {
                    // 1. Handle HTTP response status manually
                    if (!response.getStatusCode().is2xxSuccessful()) {
                        throw new RuntimeException("HTTP Request failed with status: " + response.getStatusCode());
                    }

                    // 2. Access the raw socket input stream
                    try (InputStream responseBodyStream = response.getBody()) {
                        // 3. Extract the input stream to string
                        String rawJsonResponse = StreamUtils.copyToString(responseBodyStream, StandardCharsets.UTF_8);

                        // 4. Manually deserialize JSON string back to ProductDto
                        return objectMapper.readValue(rawJsonResponse, ProductDto.class);
                    }
                }
        );
    }
}
```

---

## 4. Programming Exercises

To build practical mastery, complete the following two programmatic exercises focused on high-level validation pipelines and manual interceptor architectures.

### 4.1 Programming Exercise 1: High-Level Client Validation Pipeline using `exchange`

#### Problem Statement

You need to build a security and validation filter within the **Order Service** (running on port `8081`). When an order is placed, the Order Service must synchronously issue an HTTP POST request to the **Product Service** (running on port `8082`) at `/products/validate-batch`.

The transaction requires custom HTTP headers to pass authentication and tracing keys. If the downstream service yields success, return the payload; otherwise, handle failure statuses gracefully.

#### Context and Acceptance Criteria

1. The downstream endpoint expects a batch request payload containing product IDs and quantities, and requires custom headers `X-Auth-Token` and `X-Request-Correlation-ID`.
2. Use the `exchange` method inside Order Service to retain control over headers while relying on automated Jackson binding.
3. If Product Service is offline or returns an HTTP Error code (e.g., `401 Unauthorized` or `500 Server Error`), catch `RestClientResponseException`, log the details, and return a default fallback payload indicating a validation failure.

#### Starter Code & Hints

```java
// Target Endpoint: POST <http://localhost:8082/products/validate-batch>
public record ValidationRequest(List<Long> productIds, int requestedQuantity) {}
public record ValidationResponse(boolean validated, String denialReason) {}
```

#### Detailed Solution

#### Downstream Product Controller (Port 8082)

```java
package com.example.product.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/products")
public class ProductValidationController {

    @PostMapping("/validate-batch")
    public ResponseEntity<ValidationResponse> validateBatch(
            @RequestHeader("X-Auth-Token") String authToken,
            @RequestHeader("X-Request-Correlation-ID") String correlationId,
            @RequestBody ValidationRequest request) {

        // Validate security headers
        if (!"SECURE_TOKEN_999".equals(authToken)) {
            return ResponseEntity.status(401).build();
        }

        // Evaluate validation request
        boolean isAllValid = request.productIds().stream().allMatch(id -> id < 1000L);
        String reason = isAllValid ? "Approved" : "Rejected: Product ID exceeds system threshold";

        return ResponseEntity.ok(new ValidationResponse(isAllValid, reason));
    }
}

record ValidationRequest(List<Long> productIds, int requestedQuantity) {}
record ValidationResponse(boolean validated, String denialReason) {}
```

#### Upstream Order Service Validation (Port 8081)

```java
package com.example.order.service;

import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import java.util.List;

@Service
public class OrderBatchValidator {

    private final RestTemplate restTemplate;

    public OrderBatchValidator(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public ValidationResponse performBatchValidation(List<Long> productIds, int quantity, String token) {
        String url = "<http://localhost:8082/products/validate-batch>";

        // Create headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Auth-Token", token);
        headers.set("X-Request-Correlation-ID", "corr-77182a-993d");

        // Construct request payload
        ValidationRequest payload = new ValidationRequest(productIds, quantity);
        HttpEntity<ValidationRequest> entity = new HttpEntity<>(payload, headers);

        try {
            // Use exchange to execute POST and retrieve the complete response
            ResponseEntity<ValidationResponse> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    entity,
                    ValidationResponse.class
            );

            return response.getBody();

        } catch (RestClientResponseException ex) {
            // Handle and parse HTTP error codes gracefully
            System.err.println("Validation Service Error: " + ex.getRawStatusCode() + " - " + ex.getResponseBodyAsString());
            return new ValidationResponse(false, "Validation Service returned error status: " + ex.getRawStatusCode());
        } catch (Exception ex) {
            // Handle connection drops or offline statuses
            System.err.println("Fatal Integration Error: " + ex.getMessage());
            return new ValidationResponse(false, "Validation Service is currently offline.");
        }
    }
}

record ValidationRequest(List<Long> productIds, int requestedQuantity) {}
record ValidationResponse(boolean validated, String denialReason) {}
```

---

### 4.2 Programming Exercise 2: Manual Serialization and Extraction using `execute`

#### Problem Statement

You need to build a custom migration exporter that sends raw configuration payloads to a configuration sync host. Because the host operates on custom binary layouts containing JSON arrays, you must implement the migration logic manually using the low-level `execute` method. You cannot use convenience methods, and you must manually write the request bytes and copy the response body input stream to string for processing.

#### Context and Acceptance Criteria

1. Target URL: `POST <http://localhost:8082/products/migrate`>
2. Use `restTemplate.execute(...)` with direct `RequestCallback` and `ResponseExtractor` lambdas.
3. Manually convert the `ProductDto` array into JSON bytes using `ObjectMapper`.
4. Write the bytes directly into the request body stream.
5. In the response extractor, read the raw `InputStream` from `ClientHttpResponse` using `StreamUtils.copyToString` in UTF-8.
6. Deserialize the final JSON string back into a `MigrationReport` class.

#### Starter Code & Hints

```java
public record MigrationReport(int processedCount, boolean success, String statusNotes) {}
```

#### Detailed Solution

#### Migration Service Implementation (Port 8081)

```java
package com.example.order.service;

import com.example.communication.model.ProductDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestTemplate;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Service
public class ManualMigrationService {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public ManualMigrationService(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public MigrationReport executeProductMigration(List<ProductDto> productsToMigrate) {
        String url = "<http://localhost:8082/products/migrate>";

        try {
            return restTemplate.execute(
                    url,
                    HttpMethod.POST,
                    // RequestCallback lambda
                    request -> {
                        // Manually configure custom system header metadata
                        request.getHeaders().setContentType(MediaType.APPLICATION_JSON);
                        request.getHeaders().set("X-Migration-Mode", "BATCH_BULK");

                        // Manually serialize the List of products to a JSON byte array
                        byte[] payloadBytes = objectMapper.writeValueAsBytes(productsToMigrate);

                        // Write serialized payload directly to network stream
                        request.getBody().write(payloadBytes);
                    },
                    // ResponseExtractor lambda
                    response -> {
                        // Check if server rejected connection
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            return new MigrationReport(0, false, "Migration Server HTTP Error: " + response.getStatusCode().value());
                        }

                        // Retrieve response input stream and read raw data safely
                        try (InputStream is = response.getBody()) {
                            String rawJson = StreamUtils.copyToString(is, StandardCharsets.UTF_8);

                            // Deserialise back to target payload object
                            return objectMapper.readValue(rawJson, MigrationReport.class);
                        }
                    }
            );
        } catch (Exception ex) {
            System.err.println("Manual Migration Exception: " + ex.getMessage());
            return new MigrationReport(0, false, "Migration failed: " + ex.getMessage());
        }
    }
}

record MigrationReport(int processedCount, boolean success, String statusNotes) {}
```

---

### 4.3 Clarified Nuances and Edge Cases

1. **Manual Resource Leak Mitigation**: In the `ResponseExtractor` lambda, the raw response stream `response.getBody()` must always be handled within a try-with-resources statement. If the stream is left unclosed, the underlying HTTP socket remains in use, blocking JVM connection pools and causing system thread starvation.
2. **No Automatic Conversion Exceptions**: When utilizing `execute`, Spring will not automatically catch mapping anomalies or throw standard `RestClientException` types if JSON strings are malformed. All mapping errors must be caught manually or processed inside the caller’s context.
3. **Automatic Header Appends**: Even inside low-level `RequestCallback` execution blocks, Spring's underlying connection factories may inject generic headers (e.g. `Accept-Length` or `Host`). Custom headers set in the callback block will overwrite these default settings.