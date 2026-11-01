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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpStatus;
import org.openhab.binding.nuki.internal.constants.NukiBindingConstants;
import org.openhab.binding.nuki.internal.constants.NukiLinkBuilder;
import org.openhab.binding.nuki.internal.constants.OpenerAction;
import org.openhab.binding.nuki.internal.constants.SmartLockAction;
import org.openhab.binding.nuki.internal.dto.BridgeApiCallbackAddDto;
import org.openhab.binding.nuki.internal.dto.BridgeApiCallbackListDto;
import org.openhab.binding.nuki.internal.dto.BridgeApiCallbackRemoveDto;
import org.openhab.binding.nuki.internal.dto.BridgeApiInfoDto;
import org.openhab.binding.nuki.internal.dto.BridgeApiListDeviceDto;
import org.openhab.binding.nuki.internal.dto.BridgeApiLockActionDto;
import org.openhab.binding.nuki.internal.dto.BridgeApiLockStateDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * The {@link NukiHttpClient} class is responsible for getting data from the Nuki Bridge.
 *
 * @author Markus Katter - Initial contribution
 * @author Jan Vybíral - Hashed token authentication
 * @author Reto Hui - Request timeouts so a hung bridge cannot block the binding
 */
@NonNullByDefault
public class NukiHttpClient {

    /**
     * Timeout for bridge-local calls ({@code /info}, {@code /list}, callbacks). A healthy bridge answers in well
     * under a second; this only bounds a connection that never completes.
     */
    static final long BRIDGE_REQUEST_TIMEOUT_MS = 10_000;

    /**
     * Timeout for {@code /lockState} and {@code /lockAction}, which wait for Bluetooth. Long enough for a slow
     * lock, short enough that one stuck call cannot wedge every other command.
     */
    static final long DEVICE_REQUEST_TIMEOUT_MS = 45_000;

    private final Logger logger = LoggerFactory.getLogger(NukiHttpClient.class);

    private final HttpClient httpClient;
    private final Gson gson;
    private final NukiLinkBuilder linkBuilder;
    private final long bridgeTimeoutMs;
    private final long deviceTimeoutMs;
    /**
     * The bridge HTTP API handles one request at a time and answers 503 when another is in progress.
     * Requests are serialized so callers do not trip that limit. The lock is never held longer than the
     * request timeout, and {@link #close()} aborts the in-flight call so disposal does not wait for it.
     */
    private final ReentrantLock requestLock = new ReentrantLock();
    private final AtomicReference<@Nullable Request> inFlight = new AtomicReference<>();
    private final ThreadLocal<@Nullable Long> timeoutOverride = new ThreadLocal<>();
    private volatile boolean closed;

    public NukiHttpClient(HttpClient httpClient, NukiLinkBuilder linkBuilder) {
        this(httpClient, linkBuilder, BRIDGE_REQUEST_TIMEOUT_MS, DEVICE_REQUEST_TIMEOUT_MS);
    }

    NukiHttpClient(HttpClient httpClient, NukiLinkBuilder linkBuilder, long bridgeTimeoutMs, long deviceTimeoutMs) {
        logger.debug("Instantiating NukiHttpClient");
        this.httpClient = httpClient;
        this.linkBuilder = linkBuilder;
        this.bridgeTimeoutMs = bridgeTimeoutMs;
        this.deviceTimeoutMs = deviceTimeoutMs;
        gson = new Gson();
    }

    /**
     * Rejects further requests and aborts the one in flight. Safe to call while another thread is blocked in a
     * request.
     */
    public void close() {
        closed = true;
        abortInFlight();
    }

    /**
     * Aborts the in-flight request, runs {@code action} if the bridge lock is free within {@code lockWaitMs}, then
     * closes the client. {@code action} is invoked on this thread and may use this client; its requests use
     * {@code requestTimeoutMs} instead of the normal timeouts. Used when disposing a bridge so callback removal
     * cannot wait out a stuck call.
     *
     * @return {@code false} when {@code action} was skipped because the lock was not acquired in time
     */
    public boolean closeAfter(long lockWaitMs, long requestTimeoutMs, Runnable action) {
        abortInFlight();
        boolean locked = false;
        boolean ran = false;
        try {
            locked = requestLock.tryLock(lockWaitMs, TimeUnit.MILLISECONDS);
            if (!locked) {
                logger.debug("Nuki bridge request did not finish within {} ms; skipping disposal callback cleanup",
                        lockWaitMs);
                return false;
            }
            @Nullable
            Long previous = timeoutOverride.get();
            timeoutOverride.set(requestTimeoutMs);
            try {
                action.run();
                ran = true;
            } finally {
                if (previous == null) {
                    timeoutOverride.remove();
                } else {
                    timeoutOverride.set(previous);
                }
            }
            return true;
        } catch (InterruptedException e) {
            logger.debug("Interrupted while disposing the Nuki HTTP client");
            return ran;
        } finally {
            closed = true;
            abortInFlight();
            if (locked) {
                requestLock.unlock();
            }
        }
    }

    private void abortInFlight() {
        @Nullable
        Request request = inFlight.get();
        if (request != null) {
            request.abort(new TimeoutException("Nuki bridge request aborted"));
        }
    }

    private ContentResponse executeBridgeRequest(URI uri)
            throws InterruptedException, ExecutionException, TimeoutException {
        return executeRequest(uri, bridgeTimeoutMs);
    }

    private ContentResponse executeDeviceRequest(URI uri)
            throws InterruptedException, ExecutionException, TimeoutException {
        return executeRequest(uri, deviceTimeoutMs);
    }

    private ContentResponse executeRequest(URI uri, long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (closed) {
            throw new TimeoutException("Nuki HTTP client is closed");
        }
        requestLock.lockInterruptibly();
        try {
            if (closed) {
                throw new TimeoutException("Nuki HTTP client is closed");
            }
            @Nullable
            Long override = timeoutOverride.get();
            long effectiveTimeout = override != null ? override.longValue() : timeoutMs;
            logger.debug("executeRequest({}, timeout={}ms)", uri, effectiveTimeout);
            Request request = httpClient.newRequest(uri).timeout(effectiveTimeout, TimeUnit.MILLISECONDS)
                    .idleTimeout(effectiveTimeout, TimeUnit.MILLISECONDS);
            // Publish the request before checking closed again so close() cannot miss it.
            inFlight.set(request);
            if (closed) {
                request.abort(new TimeoutException("Nuki HTTP client is closed"));
                throw new TimeoutException("Nuki HTTP client is closed");
            }
            try {
                ContentResponse contentResponse = request.send();
                logger.debug("contentResponseAsString[{}]", contentResponse.getContentAsString());
                return contentResponse;
            } finally {
                inFlight.compareAndSet(request, null);
            }
        } finally {
            requestLock.unlock();
        }
    }

    private NukiBaseResponse handleException(Exception e) {
        Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        if (cause instanceof HttpResponseException causeException) {
            int status = causeException.getResponse().getStatus();
            String reason = causeException.getResponse().getReason();
            logger.debug("HTTP Response Exception! Status[{}] - Reason[{}]! Check your API Token!", status, reason);
            return new NukiBaseResponse(status, reason);
        } else if (cause instanceof InterruptedException || cause instanceof InterruptedIOException) {
            // The interrupt is consumed on purpose. These calls run on the shared scheduler, and leaving the
            // pool thread interrupted would fail the next task. Disposal is coordinated by closing this client.
            logger.debug(
                    "Nuki bridge request was interrupted. Check IP/Port configuration and if the Nuki Bridge is powered on.");
            return new NukiBaseResponse(HttpStatus.REQUEST_TIMEOUT_408,
                    "Request interrupted! Check IP/Port configuration and if Nuki Bridge is powered on!");
        } else if (cause instanceof TimeoutException) {
            logger.debug("Nuki bridge request timed out: {}", cause.toString());
            return new NukiBaseResponse(HttpStatus.REQUEST_TIMEOUT_408, "Timeout while calling the Nuki bridge");
        } else if (cause instanceof SocketException) {
            logger.debug(
                    "SocketException! Exception[{}]! Check IP/Port configuration and if Nuki Bridge is powered on!",
                    cause.getMessage());
            return new NukiBaseResponse(HttpStatus.NOT_FOUND_404,
                    "SocketException! Check IP/Port configuration and if Nuki Bridge is powered on!");
        } else if (cause instanceof IOException) {
            logger.debug("Nuki bridge request failed: {}", cause.toString());
            return new NukiBaseResponse(HttpStatus.REQUEST_TIMEOUT_408,
                    "Could not reach the Nuki bridge. Check IP/Port configuration and if it is powered on!");
        }
        logger.error("Could not handle Exception! Exception[{}]", e.getMessage(), e);
        return new NukiBaseResponse(HttpStatus.INTERNAL_SERVER_ERROR_500, e.getMessage());
    }

    public BridgeInfoResponse getBridgeInfo() {
        logger.debug("getBridgeInfo() in thread {}", Thread.currentThread().threadId());
        try {
            ContentResponse contentResponse = executeBridgeRequest(linkBuilder.info());
            int status = contentResponse.getStatus();
            String response = contentResponse.getContentAsString();
            logger.debug("getBridgeInfo status[{}] response[{}]", status, response);
            if (status == HttpStatus.OK_200) {
                BridgeApiInfoDto bridgeApiInfoDto = gson.fromJson(response, BridgeApiInfoDto.class);
                logger.debug("getBridgeInfo OK");
                return new BridgeInfoResponse(status, contentResponse.getReason(), bridgeApiInfoDto);
            } else {
                logger.debug("Could not get Bridge Info! Status[{}] - Response[{}]", status, response);
                return new BridgeInfoResponse(status, contentResponse.getReason(), null);
            }
        } catch (Exception e) {
            logger.debug("Could not get Bridge Info! Exception[{}]", e.getMessage());
            return new BridgeInfoResponse(handleException(e));
        }
    }

    public BridgeListResponse getList() {
        logger.debug("getList() in thread {}", Thread.currentThread().threadId());
        try {
            ContentResponse contentResponse = executeBridgeRequest(linkBuilder.list());
            int status = contentResponse.getStatus();
            String response = contentResponse.getContentAsString();
            logger.debug("getList status[{}] response[{}]", status, response);
            if (status == HttpStatus.OK_200) {
                BridgeApiListDeviceDto[] bridgeApiInfoDtoArray = gson.fromJson(response,
                        BridgeApiListDeviceDto[].class);
                logger.debug("getList OK");
                return new BridgeListResponse(status, contentResponse.getReason(),
                        Arrays.asList(bridgeApiInfoDtoArray));
            } else {
                logger.debug("Could not get Bridge Info! Status[{}] - Response[{}]", status, response);
                return new BridgeListResponse(status, contentResponse.getReason(), null);
            }
        } catch (Exception e) {
            logger.debug("Could not get List! Exception[{}]", e.getMessage());
            return new BridgeListResponse(handleException(e));
        }
    }

    public BridgeLockStateResponse getBridgeLockState(String nukiId, int deviceType) {
        logger.debug("getBridgeLockState({}) in thread {}", nukiId, Thread.currentThread().threadId());

        try {
            ContentResponse contentResponse = executeDeviceRequest(linkBuilder.lockState(nukiId, deviceType));
            int status = contentResponse.getStatus();
            String response = contentResponse.getContentAsString();
            logger.debug("getBridgeLockState status[{}] response[{}]", status, response);
            if (status == HttpStatus.OK_200) {
                BridgeApiLockStateDto bridgeApiLockStateDto = gson.fromJson(response, BridgeApiLockStateDto.class);
                logger.debug("getBridgeLockState OK");
                return new BridgeLockStateResponse(status, contentResponse.getReason(), bridgeApiLockStateDto);
            } else {
                logger.debug("Could not get Lock State! Status[{}] - Response[{}]", status, response);
                return new BridgeLockStateResponse(status, contentResponse.getReason(), null);
            }
        } catch (Exception e) {
            logger.debug("Could not get Bridge Lock State!", e);
            return new BridgeLockStateResponse(handleException(e));
        }
    }

    public BridgeLockActionResponse getSmartLockAction(String nukiId, SmartLockAction action, int deviceType) {
        return getBridgeLockAction(nukiId, action.getAction(), deviceType);
    }

    public BridgeLockActionResponse getOpenerAction(String nukiId, OpenerAction action) {
        return getBridgeLockAction(nukiId, action.getAction(), NukiBindingConstants.DEVICE_OPENER);
    }

    private BridgeLockActionResponse getBridgeLockAction(String nukiId, int lockAction, int deviceType) {
        logger.debug("getBridgeLockAction({}, {}) in thread {}", nukiId, lockAction, Thread.currentThread().threadId());
        try {
            ContentResponse contentResponse = executeDeviceRequest(
                    linkBuilder.lockAction(nukiId, deviceType, lockAction));
            int status = contentResponse.getStatus();
            String response = contentResponse.getContentAsString();
            logger.debug("getBridgeLockAction status[{}] response[{}]", status, response);
            if (status == HttpStatus.OK_200) {
                BridgeApiLockActionDto bridgeApiLockActionDto = gson.fromJson(response, BridgeApiLockActionDto.class);
                logger.debug("getBridgeLockAction OK");
                return new BridgeLockActionResponse(status, contentResponse.getReason(), bridgeApiLockActionDto);
            } else {
                logger.debug("Could not execute Lock Action! Status[{}] - Response[{}]", status, response);
                return new BridgeLockActionResponse(status, contentResponse.getReason(), null);
            }
        } catch (Exception e) {
            logger.debug("Could not execute Lock Action! Exception[{}]", e.getMessage());
            return new BridgeLockActionResponse(handleException(e));
        }
    }

    public BridgeCallbackAddResponse getBridgeCallbackAdd(String callbackUrl) {
        logger.debug("getBridgeCallbackAdd({}) in thread {}", callbackUrl, Thread.currentThread().threadId());
        try {
            ContentResponse contentResponse = executeBridgeRequest(linkBuilder.callbackAdd(callbackUrl));
            int status = contentResponse.getStatus();
            String response = contentResponse.getContentAsString();
            logger.debug("getBridgeCallbackAdd status[{}] response[{}]", status, response);
            if (status == HttpStatus.OK_200) {
                BridgeApiCallbackAddDto bridgeApiCallbackAddDto = gson.fromJson(response,
                        BridgeApiCallbackAddDto.class);
                logger.debug("getBridgeCallbackAdd OK");
                return new BridgeCallbackAddResponse(status, contentResponse.getReason(), bridgeApiCallbackAddDto);
            } else {
                logger.debug("Could not execute Callback Add! Status[{}] - Response[{}]", status, response);
                return new BridgeCallbackAddResponse(status, contentResponse.getReason(), null);
            }
        } catch (Exception e) {
            logger.debug("Could not execute Callback Add! Exception[{}]", e.getMessage());
            return new BridgeCallbackAddResponse(handleException(e));
        }
    }

    public BridgeCallbackListResponse getBridgeCallbackList() {
        logger.debug("getBridgeCallbackList() in thread {}", Thread.currentThread().threadId());
        try {
            ContentResponse contentResponse = executeBridgeRequest(linkBuilder.callbackList());
            int status = contentResponse.getStatus();
            String response = contentResponse.getContentAsString();
            logger.debug("getBridgeCallbackList status[{}] response[{}]", status, response);
            if (status == HttpStatus.OK_200) {
                BridgeApiCallbackListDto bridgeApiCallbackListDto = gson.fromJson(response,
                        BridgeApiCallbackListDto.class);
                logger.debug("getBridgeCallbackList OK");
                return new BridgeCallbackListResponse(status, contentResponse.getReason(), bridgeApiCallbackListDto);
            } else {
                logger.debug("Could not execute Callback List! Status[{}] - Response[{}]", status, response);
                return new BridgeCallbackListResponse(status, contentResponse.getReason(), null);
            }
        } catch (Exception e) {
            logger.debug("Could not execute Callback List! Exception[{}]", e.getMessage());
            return new BridgeCallbackListResponse(handleException(e));
        }
    }

    public BridgeCallbackRemoveResponse getBridgeCallbackRemove(int id) {
        logger.debug("getBridgeCallbackRemove({}) in thread {}", id, Thread.currentThread().threadId());
        try {
            ContentResponse contentResponse = executeBridgeRequest(linkBuilder.callbackRemove(id));
            int status = contentResponse.getStatus();
            String response = contentResponse.getContentAsString();
            logger.debug("getBridgeCallbackRemove status[{}] response[{}]", status, response);
            if (status == HttpStatus.OK_200) {
                BridgeApiCallbackRemoveDto bridgeApiCallbackRemoveDto = gson.fromJson(response,
                        BridgeApiCallbackRemoveDto.class);
                logger.debug("getBridgeCallbackRemove OK");
                return new BridgeCallbackRemoveResponse(status, contentResponse.getReason(),
                        bridgeApiCallbackRemoveDto);
            } else {
                logger.debug("Could not execute Callback Remove! Status[{}] - Response[{}]", status, response);
                return new BridgeCallbackRemoveResponse(status, contentResponse.getReason(), null);
            }
        } catch (Exception e) {
            logger.debug("Could not execute Callback Remove! Exception[{}]", e.getMessage());
            return new BridgeCallbackRemoveResponse(handleException(e));
        }
    }
}
