/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.rest.v1;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.rest.RESTServer;
import io.pravega.controller.server.rest.RESTServerConfig;
import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsList;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.pravega.controller.server.rest.generated.model.ScopesList;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rest.generated.model.StreamsList;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.controller.server.rest.impl.RESTServerConfigImpl;
import io.pravega.controller.server.rpc.auth.PravegaAuthManager;
import io.pravega.controller.store.stream.ScaleMetadata;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestUtils;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.controller.server.rest.generated.model.RetentionConfig.TypeEnum;
import static io.pravega.shared.NameUtils.getStreamForReaderGroup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for Stream metadata REST APIs.
 */
@Slf4j
public class StreamMetaDataTests {

    //Ensure each test completes within 30 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    protected final String scope1 = "scope1";
    protected final CreateStreamRequest createStreamRequest = new CreateStreamRequest();
    protected final UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest();
    protected ControllerService mockControllerService;
    protected PravegaAuthManager authManager = null;
    protected Client client;

    private RESTServerConfig serverConfig;
    private RESTServer restServer;
    private ConnectionFactoryImpl connectionFactory;
    
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final String stream3 = "stream3";
    private final String stream4 = "stream4";

    private final ScalingConfig scalingPolicyCommon = new ScalingConfig();
    private final ScalingConfig scalingPolicyCommon2 = new ScalingConfig();
    private final RetentionConfig retentionPolicyCommon = new RetentionConfig();
    private final RetentionConfig retentionPolicyCommon2 = new RetentionConfig();
    private final StreamProperty streamResponseExpected = new StreamProperty();
    private final StreamProperty streamResponseExpected2 = new StreamProperty();
    private final StreamProperty streamResponseExpected3 = new StreamProperty();
    private final StreamConfiguration streamConfiguration = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
            .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(123L)))
            .build();

    private final CreateStreamRequest createStreamRequest2 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest3 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest4 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest5 = new CreateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest2 = new UpdateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest3 = new UpdateStreamRequest();

    private final CompletableFuture<StreamConfiguration> streamConfigFuture = CompletableFuture.
            completedFuture(streamConfiguration);
    private final CompletableFuture<CreateStreamStatus> createStreamStatus = CompletableFuture.
            completedFuture(CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.SUCCESS).build());
    private final CompletableFuture<CreateStreamStatus> createStreamStatus2 = CompletableFuture.
            completedFuture(CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.STREAM_EXISTS).build());
    private final CompletableFuture<CreateStreamStatus> createStreamStatus3 = CompletableFuture.
            completedFuture(CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.FAILURE).build());
    private final CompletableFuture<CreateStreamStatus> createStreamStatus4 = CompletableFuture.
            completedFuture(CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.SCOPE_NOT_FOUND).build());
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus = CompletableFuture.
            completedFuture(UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SUCCESS).build());
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus2 = CompletableFuture.
            completedFuture(UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.STREAM_NOT_FOUND).build());
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus3 = CompletableFuture.
            completedFuture(UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.FAILURE).build());
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus4 = CompletableFuture.
            completedFuture(UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build());

    @Before
    public void setup() throws Exception {
        mockControllerService = mock(ControllerService.class);
        serverConfig = RESTServerConfigImpl.builder().host("localhost").port(TestUtils.getAvailableListenPort()).build();
        LocalController controller = new LocalController(mockControllerService, false, "");
        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                        .controllerURI(URI.create("tcp://localhost"))
                                                                                        .build());
        restServer = new RESTServer(controller, mockControllerService, authManager, serverConfig,
                connectionFactory);
        restServer.startAsync();
        restServer.awaitRunning();
        client = ClientBuilder.newClient();

        scalingPolicyCommon.setType(ScalingConfig.TypeEnum.BY_RATE_IN_EVENTS_PER_SEC);
        scalingPolicyCommon.setTargetRate(100);
        scalingPolicyCommon.setScaleFactor(2);
        scalingPolicyCommon.setMinSegments(2);

        scalingPolicyCommon2.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingPolicyCommon2.setMinSegments(2);

        retentionPolicyCommon.setType(TypeEnum.LIMITED_DAYS);
        retentionPolicyCommon.setValue(123L);

        retentionPolicyCommon2.setType(null);
        retentionPolicyCommon2.setValue(null);

        streamResponseExpected.setScopeName(scope1);
        streamResponseExpected.setStreamName(stream1);
        streamResponseExpected.setScalingPolicy(scalingPolicyCommon);
        streamResponseExpected.setRetentionPolicy(retentionPolicyCommon);

        createStreamRequest.setStreamName(stream1);
        createStreamRequest.setScalingPolicy(scalingPolicyCommon);
        createStreamRequest.setRetentionPolicy(retentionPolicyCommon);

        createStreamRequest2.setStreamName(stream1);
        createStreamRequest2.setScalingPolicy(scalingPolicyCommon);
        createStreamRequest2.setRetentionPolicy(retentionPolicyCommon2);

        createStreamRequest3.setStreamName(stream1);
        createStreamRequest3.setScalingPolicy(scalingPolicyCommon);
        createStreamRequest3.setRetentionPolicy(retentionPolicyCommon);

        createStreamRequest4.setStreamName(stream3);
        createStreamRequest4.setScalingPolicy(scalingPolicyCommon);

        // stream 4 where targetRate and scalingFactor for Scaling Policy are null
        createStreamRequest5.setStreamName(stream4);
        createStreamRequest5.setScalingPolicy(scalingPolicyCommon2);
        createStreamRequest5.setRetentionPolicy(retentionPolicyCommon);

        streamResponseExpected2.setScopeName(scope1);
        streamResponseExpected2.setStreamName(stream3);
        streamResponseExpected2.setScalingPolicy(scalingPolicyCommon);

        streamResponseExpected3.setScopeName(scope1);
        streamResponseExpected3.setStreamName(stream4);
        streamResponseExpected3.setScalingPolicy(scalingPolicyCommon2);
        streamResponseExpected3.setRetentionPolicy(retentionPolicyCommon);

        updateStreamRequest.setScalingPolicy(scalingPolicyCommon);
        updateStreamRequest.setRetentionPolicy(retentionPolicyCommon);
        updateStreamRequest2.setScalingPolicy(scalingPolicyCommon);
        updateStreamRequest2.setRetentionPolicy(retentionPolicyCommon);
        updateStreamRequest3.setScalingPolicy(scalingPolicyCommon);
        updateStreamRequest3.setRetentionPolicy(retentionPolicyCommon2);
    }

    @After
    public void tearDown() {
        client.close();
        restServer.stopAsync();
        restServer.awaitTerminated();
        connectionFactory.close();
    }

    /**
     * Test for createStream REST API.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testCreateStream() throws ExecutionException, InterruptedException {
        String streamResourceURI = getURI() + "v1/scopes/" + scope1 + "/streams";

        // Test to create a stream which doesn't exist
        when(mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(createStreamStatus);
        Response response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest)).invoke();
        assertEquals("Create Stream Status", 201, response.getStatus());
        StreamProperty streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
        response.close();

        // Test to create a stream which doesn't exist and has no Retention Policy set.
        when(mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(createStreamStatus);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest4)).invoke();
        assertEquals("Create Stream Status", 201, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected2, streamResponseActual);
        response.close();

        // Test to create a stream with internal stream name
        final CreateStreamRequest streamRequest = new CreateStreamRequest();
        streamRequest.setStreamName(NameUtils.getInternalNameForStream("stream"));
        when(mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(createStreamStatus2);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(streamRequest)).invoke();
        assertEquals("Create Stream Status", 400, response.getStatus());
        response.close();

        // Test to create a stream which doesn't exist and have Scaling Policy FIXED_NUM_SEGMENTS
        when(mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(createStreamStatus);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest5)).invoke();
        assertEquals("Create Stream Status", 201, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected3, streamResponseActual);
        response.close();

        // Test to create a stream that already exists
        when(mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(createStreamStatus2);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest)).invoke();
        assertEquals("Create Stream Status", 409, response.getStatus());
        response.close();

        // Test for validation of create stream request object
        when(mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(createStreamStatus3);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest2)).invoke();
        // TODO: Server should be returning 400 here, change this once issue
        // https://github.com/pravega/pravega/issues/531 is fixed.
        assertEquals("Create Stream Status", 500, response.getStatus());
        response.close();

        // Test create stream for non-existent scope
        when(mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(createStreamStatus4);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest3)).invoke();
        assertEquals("Create Stream Status for non-existent scope", 404, response.getStatus());
        response.close();
    }

    /**
     * Test for updateStreamConfig REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testUpdateStream() throws ExecutionException, InterruptedException {
        String resourceURI = getURI() + "v1/scopes/" + scope1 + "/streams/" + stream1;

        // Test to update an existing stream
        when(mockControllerService.updateStream(any(), any(), any())).thenReturn(updateStreamStatus);
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(updateStreamRequest)).invoke();
        assertEquals("Update Stream Status", 200, response.getStatus());
        StreamProperty streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
        response.close();

        // Test sending extra fields in the request object to check if json parser can handle it.
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(createStreamRequest)).invoke();
        assertEquals("Update Stream Status", 200, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
        response.close();

        // Test to update an non-existing stream
        when(mockControllerService.updateStream(any(), any(), any())).thenReturn(updateStreamStatus2);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(updateStreamRequest2)).invoke();
        assertEquals("Update Stream Status", 404, response.getStatus());
        response.close();

        // Test for validation of request object
        when(mockControllerService.updateStream(any(), any(), any())).thenReturn(updateStreamStatus3);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(updateStreamRequest3)).invoke();
        // TODO: Server should be returning 400 here, change this once issue
        // https://github.com/pravega/pravega/issues/531 is fixed.
        assertEquals("Update Stream Status", 500, response.getStatus());
        response.close();

        // Test to update stream for non-existent scope
        when(mockControllerService.updateStream(any(), any(), any())).thenReturn(updateStreamStatus4);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(updateStreamRequest)).invoke();
        assertEquals("Update Stream Status", 404, response.getStatus());
        response.close();
    }

    protected Invocation.Builder addAuthHeaders(Invocation.Builder request) {
        return request;
    }

    /**
     * Test for getStreamConfig REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testGetStream() throws ExecutionException, InterruptedException {
        String resourceURI = getURI() + "v1/scopes/" + scope1 + "/streams/" + stream1;
        String resourceURI2 = getURI() + "v1/scopes/" + scope1 + "/streams/" + stream2;

        // Test to get an existing stream
        when(mockControllerService.getStream(scope1, stream1)).thenReturn(streamConfigFuture);
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", 200, response.getStatus());
        StreamProperty streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
        response.close();

        // Get a non-existent stream
        when(mockControllerService.getStream(scope1, stream2)).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, stream2);
        }));
        response = addAuthHeaders(client.target(resourceURI2).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", 404, response.getStatus());
        response.close();
    }

    /**
     * Test for deleteStream REST API
     *
     * @throws Exception
     */
    @Test
    public void testDeleteStream() throws Exception {
        final String resourceURI = getURI() + "v1/scopes/scope1/streams/stream1";

        // Test to delete a sealed stream
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.SUCCESS).build()));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Stream response code", 204, response.getStatus());
        response.close();

        // Test to delete a unsealed stream
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.STREAM_NOT_SEALED).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Stream response code", 412, response.getStatus());
        response.close();

        // Test to delete a non existent stream
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.STREAM_NOT_FOUND).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Stream response code", 404, response.getStatus());
        response.close();

        // Test to delete a stream giving an internal server error
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.FAILURE).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Stream response code", 500, response.getStatus());
        response.close();
    }

    /**
     * Test for createScope REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testCreateScope() throws ExecutionException, InterruptedException {
        final CreateScopeRequest createScopeRequest = new CreateScopeRequest().scopeName(scope1);
        final String resourceURI = getURI() + "v1/scopes/";

        // Test to create a new scope.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SUCCESS).build()));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 201, response.getStatus());
        response.close();

        // Test to create an existing scope.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 409, response.getStatus());
        response.close();

        // create scope failure.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.FAILURE).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 500, response.getStatus());
        response.close();

        // Test to create an invalid scope name.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build()));
        createScopeRequest.setScopeName("_system");
        response = addAuthHeaders(client.target(resourceURI).request()).buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 400, response.getStatus());
        response.close();
    }

    /**
     * Test for deleteScope REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testDeleteScope() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes/scope1";

        // Test to delete a scope.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SUCCESS).build()));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Scope response code", 204, response.getStatus());
        response.close();

        // Test to delete scope with existing streams.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Scope response code", 412, response.getStatus());
        response.close();

        // Test to delete non-existing scope.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Scope response code", 404, response.getStatus());
        response.close();

        // Test delete scope failure.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.FAILURE).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Scope response code", 500, response.getStatus());
        response.close();
    }

    /**
     * Test to retrieve a scope.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testGetScope() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes/scope1";
        final String resourceURI2 = getURI() + "v1/scopes/scope2";

        // Test to get existent scope
        when(mockControllerService.getScope(scope1)).thenReturn(CompletableFuture.completedFuture("scope1"));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get existent scope", 200, response.getStatus());
        response.close();

        // Test to get non-existent scope
        when(mockControllerService.getScope("scope2")).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope2");
        }));
        response = addAuthHeaders(client.target(resourceURI2).request()).buildGet().invoke();
        assertEquals("Get non existent scope", 404, response.getStatus());
        response.close();

        //Test for get scope failure.
        final CompletableFuture<String> completableFuture2 = new CompletableFuture<>();
        completableFuture2.completeExceptionally(new Exception());
        when(mockControllerService.getScope(scope1)).thenReturn(completableFuture2);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get scope fail test", 500, response.getStatus());
        response.close();
    }

    /**
     * Test for listScopes REST API.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testListScopes() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes";

        // Test to list scopes.
        List<String> scopesList = Arrays.asList("scope1", "scope2", "scope3");
        when(mockControllerService.listScopes()).thenReturn(CompletableFuture.completedFuture(scopesList));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Scopes response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());

        verifyScopes(response.readEntity(ScopesList.class));

        response.close();

        // Test for list scopes failure.
        final CompletableFuture<List<String>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listScopes()).thenReturn(completableFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Scopes response code", 500, response.getStatus());
        response.close();
    }

    protected void verifyScopes(ScopesList scopesList1) {
        assertEquals(3, scopesList1.getScopes().size());
        assertEquals("scope1", scopesList1.getScopes().get(0).getScopeName());
        assertEquals("scope2", scopesList1.getScopes().get(1).getScopeName());
        assertEquals("scope3", scopesList1.getScopes().get(2).getScopeName());
    }

    /**
     * Test for listStreams REST API.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testListStreams() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes/scope1/streams";

        final StreamConfiguration streamConfiguration1 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(123L)))
                .build();

        final StreamConfiguration streamConfiguration2 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(123L)))
                .build();

        // Test to list streams.
        Map<String, StreamConfiguration> streamsList = ImmutableMap.of(stream1, streamConfiguration1, stream2, streamConfiguration2);

        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(CompletableFuture.completedFuture(streamsList));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        final StreamsList streamsList1 = response.readEntity(StreamsList.class);
        assertEquals("List count", streamsList1.getStreams().size(), 2);
        assertEquals("List element", streamsList1.getStreams().get(0).getStreamName(), "stream1");
        assertEquals("List element", streamsList1.getStreams().get(1).getStreamName(), "stream2");
        response.close();

        // Test for list streams for invalid scope.
        final CompletableFuture<Map<String, StreamConfiguration>> completableFuture1 = new CompletableFuture<>();
        completableFuture1.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope1"));
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(completableFuture1);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 404, response.getStatus());
        response.close();

        // Test for list streams failure.
        final CompletableFuture<Map<String, StreamConfiguration>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(completableFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 500, response.getStatus());
        response.close();

        // Test for filtering streams.
        final StreamConfiguration streamConfiguration3 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Map<String, StreamConfiguration> allStreamsList = ImmutableMap.of(stream1, streamConfiguration1, stream2, streamConfiguration2,
                                                                   NameUtils.getInternalNameForStream("stream3"), streamConfiguration3);
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(
                CompletableFuture.completedFuture(allStreamsList));
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        StreamsList streamsListResp = response.readEntity(StreamsList.class);
        assertEquals("List count", 2, streamsListResp.getStreams().size());
        assertEquals("List element", "stream1", streamsListResp.getStreams().get(0).getStreamName());
        assertEquals("List element", "stream2", streamsListResp.getStreams().get(1).getStreamName());
        response.close();

        response = addAuthHeaders(client.target(resourceURI).queryParam("showInternalStreams", "true").request()).buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        streamsListResp = response.readEntity(StreamsList.class);
        assertEquals("List count", 1, streamsListResp.getStreams().size());
        assertEquals("List element", NameUtils.getInternalNameForStream("stream3"),
                streamsListResp.getStreams().get(0).getStreamName());
        response.close();

        // Test to list large number of streams.
        streamsList = new HashMap<>();
        for (int i = 0; i < 50000; i++) {
            streamsList.put("stream" + i, streamConfiguration1);
        }
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(CompletableFuture.completedFuture(streamsList));
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        final StreamsList streamsList2 = response.readEntity(StreamsList.class);
        assertEquals("List count", 50000, streamsList2.getStreams().size());
        response.close();
    }

    /**
     * Test for updateStreamState REST API.
     */
    @Test
    public void testUpdateStreamState() throws Exception {
        final String resourceURI = getURI() + "v1/scopes/scope1/streams/stream1/state";

        // Test to seal a stream.
        when(mockControllerService.sealStream("scope1", "stream1")).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SUCCESS).build()));
        StreamState streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 200, response.getStatus());
        response.close();

        // Test to seal a non existent scope.
        when(mockControllerService.sealStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 404, response.getStatus());
        response.close();

        // Test to seal a non existent stream.
        when(mockControllerService.sealStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.STREAM_NOT_FOUND).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 404, response.getStatus());
        response.close();

        // Test to check failure.
        when(mockControllerService.sealStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.FAILURE).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 500, response.getStatus());
        response.close();
    }

    /**
     * Test for getScalingEvents REST API.
     */
    @Test
    public void testGetScalingEvents() throws Exception {
        String resourceURI = getURI() + "v1/scopes/scope1/streams/stream1/scaling-events";
        List<ScaleMetadata> scaleMetadataList = new ArrayList<>();

        /* Test to get scaling events

        There are 5 scale events in final list.
        Filter 'from' and 'to' is also tested here.
        Event1 is before 'from'
        Event2 is before 'from'
        Event3 and Event4 are between 'from' and 'to'
        Event5 is after 'to'
        Response contains 3 events : Event2 acts as reference event. Event3 and Event4 fall between 'from' and 'to'.
         */
        Segment segment1 = new Segment(0, 0, System.currentTimeMillis(), 0.00, 0.50);
        Segment segment2 = new Segment(1, 0, System.currentTimeMillis(), 0.50, 1.00);
        List<Segment> segmentList1 = Arrays.asList(segment1, segment2);
        ScaleMetadata scaleMetadata1 = new ScaleMetadata(System.currentTimeMillis() / 2, segmentList1, 0L, 0L);

        Segment segment3 = new Segment(2, 0, System.currentTimeMillis(), 0.00, 0.40);
        Segment segment4 = new Segment(3, 0, System.currentTimeMillis(), 0.40, 1.00);
        List<Segment> segmentList2 = Arrays.asList(segment3, segment4);
        ScaleMetadata scaleMetadata2 = new ScaleMetadata(1 + System.currentTimeMillis() / 2, segmentList2, 1L, 1L);

        long fromDateTime = System.currentTimeMillis();

        Segment segment5 = new Segment(4, 0, System.currentTimeMillis(), 0.00, 0.50);
        Segment segment6 = new Segment(5, 0, System.currentTimeMillis(), 0.50, 1.00);
        List<Segment> segmentList3 = Arrays.asList(segment5, segment6);
        ScaleMetadata scaleMetadata3 = new ScaleMetadata(System.currentTimeMillis(), segmentList3, 1L, 1L);

        Segment segment7 = new Segment(6, 0, System.currentTimeMillis(), 0.00, 0.25);
        Segment segment8 = new Segment(7, 0, System.currentTimeMillis(), 0.25, 1.00);
        List<Segment> segmentList4 = Arrays.asList(segment7, segment8);
        ScaleMetadata scaleMetadata4 = new ScaleMetadata(System.currentTimeMillis(), segmentList4, 1L, 1L);

        long toDateTime = System.currentTimeMillis();

        Segment segment9 = new Segment(8, 0, System.currentTimeMillis(), 0.00, 0.40);
        Segment segment10 = new Segment(9, 0, System.currentTimeMillis(), 0.40, 1.00);
        List<Segment> segmentList5 = Arrays.asList(segment9, segment10);
        ScaleMetadata scaleMetadata5 = new ScaleMetadata(toDateTime * 2, segmentList5, 1L, 1L);

        // HistoryRecords.readAllRecords returns a list of records in decreasing order
        // so we add the elements in reverse order as well to simulate that behavior
        scaleMetadataList.add(scaleMetadata5);
        scaleMetadataList.add(scaleMetadata4);
        scaleMetadataList.add(scaleMetadata3);
        scaleMetadataList.add(scaleMetadata2);
        scaleMetadataList.add(scaleMetadata1);

        doAnswer(x -> CompletableFuture.completedFuture(scaleMetadataList))
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong());
        Response response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime).
                queryParam("to", toDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        List<ScaleMetadata> scaleMetadataListResponse = response.readEntity(
                new GenericType<List<ScaleMetadata>>() { });
        assertEquals("List Size", 3, scaleMetadataListResponse.size());
        scaleMetadataListResponse.forEach(data -> {
            log.warn("Here");
            data.getSegments().forEach( segment -> {
               log.debug("Checking segment number: " + segment.segmentId());
               assertTrue("Event 1 shouldn't be included", segment.segmentId() != 0L);
            });
        });

        // Test for large number of scaling events.
        scaleMetadataList.clear();
        scaleMetadataList.addAll(Collections.nCopies(50000, scaleMetadata3));
        doAnswer(x -> CompletableFuture.completedFuture(scaleMetadataList))
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong());
        response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime).
                queryParam("to", toDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        scaleMetadataListResponse = response.readEntity(
                new GenericType<List<ScaleMetadata>>() { });
        assertEquals("List Size", 50000, scaleMetadataListResponse.size());

        // Test for getScalingEvents for invalid scope/stream.
        final CompletableFuture<List<ScaleMetadata>> completableFuture1 = new CompletableFuture<>();
        completableFuture1.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "stream1"));
        doAnswer(x -> completableFuture1)
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong());

        response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime).
                queryParam("to", toDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 404, response.getStatus());

        // Test for getScalingEvents for bad request.
        // 1. Missing query parameters are validated here.
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 400, response.getStatus());

        response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 400, response.getStatus());

        // 2. from > to is tested here.
        doAnswer(x -> CompletableFuture.completedFuture(scaleMetadataList))
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong());

        response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime * 2).
                queryParam("to", fromDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 400, response.getStatus());

        // Test for getScalingEvents failure.
        final CompletableFuture<List<ScaleMetadata>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        doAnswer(x -> completableFuture)
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong());

        response = addAuthHeaders(client.target(resourceURI)
                                        .queryParam("from", fromDateTime)
                                        .queryParam("to", toDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 500, response.getStatus());
    }

    /**
     * Test for listReaderGroups REST API.
     */
    @Test
    public void testListReaderGroups() {
        final String resourceURI = getURI() + "v1/scopes/scope1/readergroups";

        final StreamConfiguration streamconf1 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final StreamConfiguration streamconf2 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final StreamConfiguration readerGroup1 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final StreamConfiguration readerGroup2 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        // Fetch reader groups list.
        Map<String, StreamConfiguration> streamsList = ImmutableMap.of(stream1, streamconf1, stream2, streamconf2,
                                                                       getStreamForReaderGroup("readerGroup1"), readerGroup1,
                                                                       getStreamForReaderGroup("readerGroup2"), readerGroup2);
        when(mockControllerService.listStreamsInScope(scope1)).thenReturn(CompletableFuture.completedFuture(     streamsList));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        final ReaderGroupsList readerGroupsList1 = response.readEntity(ReaderGroupsList.class);
        assertEquals("List count", readerGroupsList1.getReaderGroups().size(), 2);
        assertEquals("List element", readerGroupsList1.getReaderGroups().get(0).getReaderGroupName(),
                "readerGroup1");
        assertEquals("List element", readerGroupsList1.getReaderGroups().get(1).getReaderGroupName(),
                "readerGroup2");
        response.close();

        // Test for list reader groups for non-existing scope.
        final CompletableFuture<Map<String, StreamConfiguration>> completableFuture1 = new CompletableFuture<>();
        completableFuture1.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope1"));
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(completableFuture1);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", 404, response.getStatus());
        response.close();

        // Test for list reader groups failure.
        final CompletableFuture<Map<String, StreamConfiguration>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(completableFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", 500, response.getStatus());
        response.close();

        // Test to list large number of reader groups.
        streamsList = new HashMap<>();
        for (int i = 0; i < 50000; i++) {
            streamsList.put(getStreamForReaderGroup("readerGroup" + i), readerGroup1);
        }
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(
                CompletableFuture.completedFuture(streamsList));
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        final ReaderGroupsList readerGroupsList = response.readEntity(ReaderGroupsList.class);
        assertEquals("List count", 50000, readerGroupsList.getReaderGroups().size());
        response.close();
    }

    private static void testExpectedVsActualObject(final StreamProperty expected, final StreamProperty actual) {
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals("StreamConfig: Scope Name ", expected.getScopeName(), actual.getScopeName());
        assertEquals("StreamConfig: Stream Name ",
                expected.getStreamName(), actual.getStreamName());
        assertEquals("StreamConfig: Scaling Policy: Type",
                expected.getScalingPolicy().getType(), actual.getScalingPolicy().getType());
        assertEquals("StreamConfig: Scaling Policy: Target Rate",
                expected.getScalingPolicy().getTargetRate(),
                actual.getScalingPolicy().getTargetRate());
        assertEquals("StreamConfig: Scaling Policy: Scale Factor",
                expected.getScalingPolicy().getScaleFactor(),
                actual.getScalingPolicy().getScaleFactor());
        assertEquals("StreamConfig: Scaling Policy: MinNumSegments",
                expected.getScalingPolicy().getMinSegments(),
                actual.getScalingPolicy().getMinSegments());
        assertEquals("StreamConfig: Retention Policy",
                expected.getRetentionPolicy(),
                actual.getRetentionPolicy());
    }

    protected String getURI() {
        return "http://localhost:" + serverConfig.getPort() + "/";
    }
}

