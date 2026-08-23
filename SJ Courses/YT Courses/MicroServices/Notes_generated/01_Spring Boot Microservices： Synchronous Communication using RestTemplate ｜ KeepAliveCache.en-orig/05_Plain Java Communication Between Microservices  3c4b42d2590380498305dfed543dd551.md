# 05_Plain Java Communication Between Microservices: Under the Hood

## **1. Low-Level HTTP Communication via Plain Java**

To understand what Spring Boot abstracts, we can use standard Java classes to initiate an outbound HTTP request from an upstream service (such as an Order Service) to a downstream service (such as a Product Service). The foundational class for this in standard Java is **`HttpURLConnection`**.

### **1.1 The Connection Lifecycle: Initialization vs. Establishment**

A common misconception is that calling **`URL.openConnection()`** immediately establishes a physical network connection. In reality, the lifecycle is divided into two distinct phases:

1. **Initialization Phase (`openConnection`)**: Calling **`url.openConnection()`** creates a Java representation of the HTTP request (the **`HttpURLConnection`** object). It acts as a configuration envelope where headers, request methods, content types, and timeouts are defined. No network packets are sent at this stage.
2. **Establishment Phase (`connect`, `getInputStream`, or `getResponseCode`)**: The actual physical TCP connection (including the 3-way handshake) and HTTP payload transmission are deferred until one of three terminal methods is invoked:
    - **`HttpURLConnection.connect()`**
    - **`HttpURLConnection.getInputStream()`**
    - **`HttpURLConnection.getResponseCode()`**

Calling **`getInputStream()`** or **`getResponseCode()`** internally detects if a connection is already open; if not, it automatically invokes **`connect()`** to build the TCP socket and send the request.

---

## **2. Configuring Sockets and Connection Timeouts**

When performing synchronous, blocking calls, resource safety requires setting strict boundaries on socket state. Without timeouts, a sluggish downstream service can block calling threads indefinitely, eventually starving the server's thread pool.

- Incoming HTTP request from UI → Our server (fixed thread pool=200) → Requires downstream app data → Server thread calls that service and waits(blocks) till it gets the response
- In a high-traffic environment, all available worker threads in the pool (e.g., all 200 threads) become exhausted within seconds.
- **Result:** The upstream service can no longer accept *any* incoming user traffic, even endpoints that do not depend on the failing downstream service, causing a total outage of the upstream service.

Standard Java provides two configuration parameters on **`HttpURLConnection`**:

### **2.1 Connection Timeout (`setConnectTimeout`)**

The connection timeout defines the maximum duration the client will wait to establish the physical TCP connection with the target server. This includes the DNS lookup and the TCP 3-way handshake.

- If the server is offline or the network route is unreachable, the client will block until this timeout is exceeded, throwing a **`java.net.SocketTimeoutException: connect timed out`**.

### **2.2 Read Timeout (`setReadTimeout`)**

The read timeout defines the maximum duration the client will wait for data after the TCP connection has been successfully established. It is the socket-level inactivity timeout.

- Once the request payload is fully transmitted, the client thread goes to sleep while waiting for the downstream service to reply. If the downstream service is executing a slow database query or hitting an infinite loop, the read timeout guarantees the client thread wakes up and throws a **`java.net.SocketTimeoutException: Read timed out`**.

---

## **3. Java HTTP Client Caching and TCP Keep-Alive**

To avoid the overhead of establishing a new TCP connection (which requires a 3-way handshake) for every single HTTP request, Java's networking stack utilizes an internal connection cache that implements HTTP/1.1 **`Keep-Alive`** behavior.

### **3.1 The Internal Connection Cache**

Java's networking subsystem maintains an internal cache of active TCP connections.

- **Key**: The cache key consists of the target host name, port number, and protocol (e.g., **`localhost:8082`**).
- **Value**: The value is an **`HttpClient`** object, which is Java's internal wrapper around the physical TCP socket.

When you call **`connect()`**, the runtime does not blindly create a new socket. It queries this internal cache:

1. **Cache Hit**: If a persistent connection to the same host and port exists and is currently idle, the runtime grabs that existing **`HttpClient`** wrapper, flips its state flag (**`inUse = true`**), and reuses the open socket.
2. **Cache Miss**: If no connection exists, or all existing connections are currently in use, a new socket is opened, and a new **`HttpClient`** is instantiated and cached.

### **3.2 The Impact of InputStream Consumption on Connection Reuse**

Whether a physical TCP socket is closed or returned to the keep-alive cache when calling **`disconnect()`** depends entirely on how the client consumed the response payload:

- **Fully Read Streams**: If the input stream is fully read by the application (i.e., read until EOF/**`1`**), calling **`disconnect()`** signals that the payload cycle is finished. The runtime sets the socket's status to idle (**`inUse = false`**) and returns the socket to the cache, keeping the TCP connection alive for subsequent requests.
- **Unread/Partially Read Streams**: If an exception occurs or the stream is closed before the entire response body is consumed, the socket is in an inconsistent state. Calling **`disconnect()`** under these conditions forces the client to physically tear down the TCP socket and close the connection to prevent data corruption.

---

## **4. Plain Java Implementation**

The following code demonstrates a manual HTTP integration using plain Java's **`HttpURLConnection`** within a standard Spring Boot controller context.

### **4.1 Upstream Service: Manual HTTP Client**

This Order Controller establishes a direct connection to a Product Service running on port **`8082`** using plain Java.

```java
package com.example.order.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @GetMapping("/via-plain-java/{productId}")
    public ResponseEntity<String> getOrderProductPlainJava(@PathVariable Long productId) {
        String targetUrl = "http://localhost:8082/products/" + productId;
        HttpURLConnection connection = null;
        InputStream inputStream = null;

        try {
            // 1. Initialize the configuration envelope
            URL url = new URL(targetUrl);
            connection = (HttpURLConnection) url.openConnection();

            // Set properties (Configuration phase)
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setConnectTimeout(5000); // 5 seconds connection timeout
            connection.setReadTimeout(5000);    // 5 seconds read timeout

            // 2. Establish connection and send request
            // getInputStream() internally invokes connect() and triggers the 3-way handshake
            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                inputStream = connection.getInputStream();

                // 3. Consume the response fully to allow TCP keep-alive reuse
                StringBuilder responseBuilder = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        responseBuilder.append(line);
                    }
                }

                String jsonResponse = responseBuilder.toString();
                return ResponseEntity.ok("Order Service received response: " + jsonResponse);
            } else {
                return ResponseEntity.status(responseCode)
                        .body("Product Service returned error code: " + responseCode);
            }

        } catch (java.net.SocketTimeoutException e) {
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                    .body("Communication timed out: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("An error occurred during communication: " + e.getMessage());
        } finally {
            // 4. Close the input stream and disconnect cleanly
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    // Log stream close failure
                }
            }
            if (connection != null) {
                // If stream was fully read, disconnect() returns the connection to the keep-alive cache
                connection.disconnect();
            }
        }
    }
}
```

---

## **5. Programming Exercise**

### **5.1 Problem Statement**

You must implement an integration component in a Spring Boot application that reads catalog information from a legacy HTTP service. Because the legacy service is unstable, you cannot use high-level Spring abstractions, and must implement the network layer using plain Java **`HttpURLConnection`** to ensure strict, custom handling of stream lifecycle and timeouts.

### **5.2 Context and Acceptance Criteria**

1. Write a service method **`public String fetchLegacyCatalog(String catalogId)`** using standard Java network utilities.
2. The endpoint to hit is **`http://legacy-system:9090/catalogs/{id}`**.
3. Configure the connection timeout to precisely **3000 milliseconds** and the read timeout to **4000 milliseconds**.
4. Set the HTTP header **`X-Client-Type`** to **`Spring-Boot-Core`**.
5. Ensure that the **`InputStream`** is fully consumed even if an HTTP error response (e.g., status codes **`400`** or **`500`**) is returned. Legacy error payloads are returned via **`HttpURLConnection.getErrorStream()`**. You must read this stream fully to release the socket back to the pool.
6. Gracefully handle socket timeout exceptions and return a fallback message.

---

### **5.3 Detailed Solution**

Below is the production-ready implementation of the legacy catalog service:

```java
package com.example.order.service;

import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Service
public class LegacyCatalogService {

    private static final String BASE_URL = "http://legacy-system:9090/catalogs/";

    public String fetchLegacyCatalog(String catalogId) {
        String targetUrl = BASE_URL + catalogId;
        HttpURLConnection connection = null;
        InputStream inputStream = null;

        try {
            URL url = new URL(targetUrl);
            connection = (HttpURLConnection) url.openConnection();

            // Configure HTTP properties
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("X-Client-Type", "Spring-Boot-Core");
            connection.setConnectTimeout(3000); // 3 seconds connect timeout
            connection.setReadTimeout(4000);    // 4 seconds read timeout

            // Fetch the response code (this initiates the connect phase)
            int responseCode = connection.getResponseCode();

            // Check if request was successful or failed
            if (responseCode >= 200 && responseCode < 300) {
                inputStream = connection.getInputStream();
            } else {
                // If error occurs, obtain the error stream to fully consume the payload
                inputStream = connection.getErrorStream();
            }

            // Consume stream fully to ensure the TCP socket is eligible for keep-alive reuse
            String resultPayload = "";
            if (inputStream != null) {
                StringBuilder responseBuilder = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        responseBuilder.append(line);
                    }
                }
                resultPayload = responseBuilder.toString();
            }

            if (responseCode >= 200 && responseCode < 300) {
                return resultPayload;
            } else {
                return "Error from legacy system (Status " + responseCode + "): " + resultPayload;
            }

        } catch (SocketTimeoutException e) {
            return "Fallback: Legacy catalog system timed out during communication. Details: " + e.getMessage();
        } catch (Exception e) {
            return "Fallback: Failed to communicate with catalog system due to a transport error. Details: " + e.getMessage();
        } finally {
            // Clean up streams and connection
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    // Suppressed
                }
            }
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
```

---

### **5.4 Clarified Nuances and Edge Cases**

1. **The Error Stream Trap**: Calling **`connection.getInputStream()`** when the server responds with an error status (such as **`500 Internal Server Error`** or **`404 Not Found`**) will throw a **`java.io.IOException`**. Developers often catch this and abort, leaving the socket unread. To maintain connection pooling under error conditions, you must invoke **`connection.getErrorStream()`**, read it to completion, and then close it.
2. **Explicit InputStream Close**: If the **`InputStream`** is not explicitly closed, the underlying file descriptors remain open, potentially leading to a resource leak (socket exhaustion) over time, even if **`connection.disconnect()`** is called.
3. **HTTP/1.1 Keep-Alive Verification**: The JVM manages the reuse pool silently. To verify that connections are being reused, you can set the system property **`Dhttp.keepAlive=true`** and adjust the maximum idle connections using **`Dhttp.maxConnections=5`**. If you fail to read streams to EOF, thread analysis will show that new TCP sockets are negotiated for every single transactional request.