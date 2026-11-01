/*
 * Copyright (c) 2010-2026 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.binding.nuki.internal.dataexchange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.openhab.binding.nuki.internal.constants.NukiLinkBuilder;

/**
 * Verifies that a bridge which accepts a connection but never answers cannot block later calls or disposal.
 *
 * @author Reto Hui - Initial contribution
 */
@NonNullByDefault
@Timeout(value = 20)
public class NukiHttpClientTest {

    private static final long HUNG_TIMEOUT_MS = 30_000;

    private final HttpClient httpClient = new HttpClient();
    private final ExecutorService connectionThreads = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "nuki-test-bridge");
        thread.setDaemon(true);
        return thread;
    });

    private @Nullable ServerSocket serverSocket;
    private @Nullable Thread acceptor;
    private volatile boolean hang;
    private final List<Socket> sockets = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger activeConnections = new AtomicInteger();
    private final AtomicInteger maxConnections = new AtomicInteger();
    private final CountDownLatch firstAccepted = new CountDownLatch(1);

    @BeforeEach
    public void setUp() throws Exception {
        httpClient.setConnectTimeout(2_000);
        httpClient.setIdleTimeout(60_000);
        httpClient.start();
        ServerSocket server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
        serverSocket = server;
        Thread thread = new Thread(() -> acceptLoop(server), "nuki-test-accept");
        thread.setDaemon(true);
        acceptor = thread;
        thread.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        ServerSocket server = serverSocket;
        if (server != null) {
            server.close();
        }
        for (Socket socket : sockets) {
            socket.close();
        }
        Thread thread = acceptor;
        if (thread != null) {
            thread.join(2_000);
        }
        connectionThreads.shutdownNow();
        httpClient.stop();
    }

    @Test
    public void hungRequestTimesOutAndDoesNotBlockTheNextCall() throws Exception {
        hang = true;
        NukiHttpClient client = newClient(500, 500);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            long startedAt = System.nanoTime();
            Future<BridgeInfoResponse> first = callers.submit(client::getBridgeInfo);
            assertTrue(firstAccepted.await(5, TimeUnit.SECONDS), "bridge did not accept the first request");
            Future<BridgeInfoResponse> second = callers.submit(client::getBridgeInfo);

            BridgeInfoResponse firstResponse = await(first, 5);
            BridgeInfoResponse secondResponse = await(second, 5);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

            assertEquals(HttpStatus.REQUEST_TIMEOUT_408, firstResponse.getStatus());
            assertEquals(HttpStatus.REQUEST_TIMEOUT_408, secondResponse.getStatus());
            assertTrue(elapsedMs < 4_000, "serialized requests stayed blocked for " + elapsedMs + " ms");
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    public void closeAbortsHungRequestAndRejectsLaterCalls() throws Exception {
        hang = true;
        NukiHttpClient client = newClient(HUNG_TIMEOUT_MS, HUNG_TIMEOUT_MS);
        ExecutorService callers = Executors.newSingleThreadExecutor();
        try {
            Future<BridgeInfoResponse> hung = callers.submit(client::getBridgeInfo);
            assertTrue(firstAccepted.await(5, TimeUnit.SECONDS));

            long startedAt = System.nanoTime();
            client.close();
            BridgeInfoResponse aborted = await(hung, 3);
            BridgeInfoResponse rejected = client.getBridgeInfo();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

            assertEquals(HttpStatus.REQUEST_TIMEOUT_408, aborted.getStatus());
            assertEquals(HttpStatus.REQUEST_TIMEOUT_408, rejected.getStatus());
            assertTrue(elapsedMs < 3_000, "close() left callers blocked for " + elapsedMs + " ms");
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    public void closeAfterReturnsWithoutWaitingForHungRequestTimeout() throws Exception {
        hang = true;
        NukiHttpClient client = newClient(HUNG_TIMEOUT_MS, HUNG_TIMEOUT_MS);
        ExecutorService callers = Executors.newSingleThreadExecutor();
        try {
            Future<BridgeInfoResponse> hung = callers.submit(client::getBridgeInfo);
            assertTrue(firstAccepted.await(5, TimeUnit.SECONDS));

            long startedAt = System.nanoTime();
            boolean cleanedUp = client.closeAfter(1_000, 400, client::getBridgeCallbackList);
            BridgeInfoResponse aborted = await(hung, 3);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

            assertTrue(cleanedUp, "callback cleanup should run once the hung request is aborted");
            assertEquals(HttpStatus.REQUEST_TIMEOUT_408, aborted.getStatus());
            assertTrue(elapsedMs < 4_000, "closeAfter blocked for " + elapsedMs + " ms");
            assertEquals(HttpStatus.REQUEST_TIMEOUT_408, client.getBridgeInfo().getStatus());
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    public void requestsAreSerialized() throws Exception {
        hang = false;
        NukiHttpClient client = newClient(2_000, 2_000);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Future<BridgeCallbackListResponse> first = callers.submit(client::getBridgeCallbackList);
            Future<BridgeCallbackListResponse> second = callers.submit(client::getBridgeCallbackList);
            assertTrue(await(first, 5).isSuccess());
            assertTrue(await(second, 5).isSuccess());
            assertEquals(1, maxConnections.get(), "bridge requests must not run in parallel");
        } finally {
            callers.shutdownNow();
        }
    }

    @SuppressWarnings("null")
    private static <T> T await(Future<T> future, long timeoutSeconds) throws Exception {
        return future.get(timeoutSeconds, TimeUnit.SECONDS);
    }

    private NukiHttpClient newClient(long bridgeTimeoutMs, long deviceTimeoutMs) throws IOException {
        ServerSocket server = serverSocket;
        if (server == null) {
            throw new IllegalStateException("server not started");
        }
        NukiLinkBuilder links = new NukiLinkBuilder("127.0.0.1", server.getLocalPort(), "token", false);
        return new NukiHttpClient(httpClient, links, bridgeTimeoutMs, deviceTimeoutMs);
    }

    private void acceptLoop(ServerSocket server) {
        while (!server.isClosed()) {
            try {
                Socket socket = server.accept();
                firstAccepted.countDown();
                connectionThreads.execute(() -> handleConnection(socket));
            } catch (IOException e) {
                break;
            }
        }
    }

    private void handleConnection(Socket socket) {
        sockets.add(socket);
        int current = activeConnections.incrementAndGet();
        maxConnections.accumulateAndGet(current, Math::max);
        try {
            if (hang) {
                socket.setSoTimeout(1_000);
                drainHeaders(socket);
                socket.setSoTimeout(0);
                InputStream input = socket.getInputStream();
                while (input.read() != -1) {
                    // Hold the connection open until the client aborts or the test stops.
                }
            } else {
                // Drop the overlap count before writing the response. The caller still holds the client lock
                // until the response arrives, so a second request cannot be accepted until this one is finished.
                activeConnections.decrementAndGet();
                writeJson(socket, "{\"callbacks\":[]}");
            }
        } catch (IOException e) {
            // The client aborted or the socket was closed with the server.
        } finally {
            if (hang) {
                activeConnections.decrementAndGet();
            }
            try {
                socket.close();
            } catch (IOException e) {
                // Already closed.
            }
        }
    }

    private static void drainHeaders(Socket socket) throws IOException {
        InputStream input = socket.getInputStream();
        byte[] buffer = new byte[512];
        StringBuilder headers = new StringBuilder();
        try {
            int read;
            while ((read = input.read(buffer)) != -1) {
                headers.append(new String(buffer, 0, read, StandardCharsets.US_ASCII));
                if (headers.indexOf("\r\n\r\n") >= 0) {
                    return;
                }
            }
        } catch (IOException e) {
            // Headers may already have been consumed, or the client gave up.
        }
    }

    private static void writeJson(Socket socket, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        byte[] header = ("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + body.length
                + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
        OutputStream output = socket.getOutputStream();
        output.write(header);
        output.write(body);
        output.flush();
    }
}
