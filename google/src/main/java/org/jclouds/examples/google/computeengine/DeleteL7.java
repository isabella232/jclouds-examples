/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jclouds.examples.google.computeengine;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jclouds.examples.google.computeengine.Constants.L7_GROUP_NAME;
import static org.jclouds.examples.google.computeengine.Constants.MAX_WAIT;
import static org.jclouds.examples.google.computeengine.Constants.NUM_INSTANCES;
import static org.jclouds.examples.google.computeengine.Constants.PROVIDER;
import static org.jclouds.examples.google.computeengine.Constants.ZONE;
import static org.jclouds.util.Predicates2.retry;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.predicates.NodePredicates;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApi;
import org.jclouds.googlecomputeengine.config.UserProject;
import org.jclouds.googlecomputeengine.domain.Operation;
import org.jclouds.googlecomputeengine.features.BackendServiceApi;
import org.jclouds.googlecomputeengine.features.ForwardingRuleApi;
import org.jclouds.googlecomputeengine.features.ResourceViewApi;
import org.jclouds.googlecomputeengine.features.TargetHttpProxyApi;
import org.jclouds.googlecomputeengine.features.UrlMapApi;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Atomics;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

/**
 * This example destroys the resources created in the CreateL7 example.
 */
public class DeleteL7 implements Closeable {
   /**
    * The first argument (args[0]) must be your service account email address
    * The second argument (args[1]) must a path to your service account
    *     private key PEM file (without a password).
    */
      
   private final ComputeService computeService;
   private static GoogleComputeEngineApi api;
   private final String project;
   private Predicate<AtomicReference<Operation>> globalOperationDonePredicate;
      
   public DeleteL7(String serviceAccountEmailAddress,
            String serviceAccountKey) {
      ComputeServiceContext context = ContextBuilder
               .newBuilder(PROVIDER)
               .credentials(serviceAccountEmailAddress, serviceAccountKey)
               .buildView(ComputeServiceContext.class);
      computeService = context.getComputeService();
      
      api = context.unwrapApi(GoogleComputeEngineApi.class);
      
      Injector injector = computeService.getContext().utils().injector();
      project = injector.getInstance(Key
               .get(new TypeLiteral<Supplier<String>>(){},
                    UserProject.class)).get();
      globalOperationDonePredicate = injector.getInstance(Key
               .get(new TypeLiteral<Predicate<AtomicReference<Operation>>>() {},
                    Names.named("global")));
   }
   
   public static void main(String[] args) throws IOException {
      String serviceAccountEmailAddress = args[0];
      String serviceAccountKey = Files.toString(new File(args[1]),
                                                Charset.defaultCharset());
      
      DeleteL7 destroyL7 = new DeleteL7(serviceAccountEmailAddress,
                                       serviceAccountKey);
      
      try {
         System.out.println("Destroying firewall");
         destroyL7.destroyFirewall(L7_GROUP_NAME + "-firewall");
         
         System.out.println("Destroying forwarding rule");
         destroyL7.destroyForwardingRule(L7_GROUP_NAME + "-forwarding-rule");
         
         System.out.println("Destroying target http proxy");
         destroyL7.destroyTargetHttpProxy(L7_GROUP_NAME + "-target-http-proxy");
         
         System.out.println("Destroying url map");
         destroyL7.destroyUrlMap(L7_GROUP_NAME + "-url-map");
         
         System.out.println("Destroying backend services");
         destroyL7.destroyBackendServices(L7_GROUP_NAME + "-backend-service-");
         
         System.out.println("Destroying resource views");
         destroyL7.destroyResourceViews(ZONE, L7_GROUP_NAME + "-resource-view-");
         
         System.out.println("Destroying nodes");
         destroyL7.destroyNodes(L7_GROUP_NAME);
         System.out.println("Nodes exterminated");
      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         destroyL7.close();
      }
   }
   
   /**
    * Deletes the nodes in the given group.
    * 
    * @param group   the name of the group to delete.
    */
   private void destroyNodes(String group) {
      computeService.destroyNodesMatching(NodePredicates
            .inGroup(group));
   }
   
   /**
    * Deletes the firewall with the given name.
    * 
    * @param name    the name of the firewall to delete.
    */
   private void destroyFirewall(String name) {
      waitGlobalOperationDone(api.getFirewallApiForProject(project)
            .delete(name), MAX_WAIT);
   }
   
   /**
    * Deletes the resource views with the given base name.
    * 
    * @param zone    the zone the resource views exist in.
    * @param baseName    the base name of the resource views to delete.
    */
   private void destroyResourceViews(String zone, String baseName) {
      ResourceViewApi resourceViewApi = api
            .getResourceViewApiForProject(project);
      for (int i = 0; i < NUM_INSTANCES; i++) {
         resourceViewApi.deleteInZone(zone, baseName + i);
      }
   }
   
   /**
    * Deletes the backend resources with the given base name.
    * 
    * @param baseName    the base name of the backend resources to delete.
    */
   private void destroyBackendServices(String baseName) {
      BackendServiceApi backendServiceApi = api
            .getBackendServiceApiForProject(project);
      for (int i = 0; i < NUM_INSTANCES; i++) {
         waitGlobalOperationDone(backendServiceApi.delete(baseName + i),
               MAX_WAIT);
      }
   }
   
   /**
    * Deletes the url map with the given name.
    * 
    * @param name    the name of the url map to delete.
    */
   private void destroyUrlMap(String name) {
      UrlMapApi urlMapApi = api
            .getUrlMapApiForProject(project);
      waitGlobalOperationDone(urlMapApi.delete(name), MAX_WAIT);
   }
   
   /**
    * Deletes the target http proxy with the given name.
    * 
    * @param name    the name of the target http proxy to delete.
    */
   private void destroyTargetHttpProxy(String name) {
      TargetHttpProxyApi targetHttpProxyApi = api
            .getTargetHttpProxyApiForProject(project);
      waitGlobalOperationDone(targetHttpProxyApi.delete(name), MAX_WAIT);
   }
   
   /**
    * Deletes the global forwarding rule with the given name.
    * 
    * @param name    the name of the global forwarding rule to delete.
    */
   private void destroyForwardingRule(String name) {
      ForwardingRuleApi forwardingRuleApi = api
            .getForwardingRuleApiForProject(project);
      waitGlobalOperationDone(forwardingRuleApi.delete(name), MAX_WAIT);
   }
   
   /**
    * Waits until the given global operation is completed.
    * 
    * @param operation        the operation to wait for.
    * @param maxWaitSeconds   the maximum number of seconds to wait.
    * @return                 the self link of the resource the operation
    *       was performed on.
    */
   private Operation waitGlobalOperationDone(Operation operation,
         long maxWaitSeconds) {
      return waitOperationDone(globalOperationDonePredicate, operation,
                               maxWaitSeconds);
   }
   
   /**
    * Waits until the given Predicate is satisfied or the maximum time is
    * reached.
    * 
    * @param operationDonePredicate    Predicate to wait for.
    * @param operation                 Operation checked against Predicate.
    * @param maxWaitSeconds            Maximum time to wait for the
    *       operation.
    * @return                          the self link of the resource the
    *       operation was performed on.
    */
   private static Operation waitOperationDone(
         Predicate<AtomicReference<Operation>> operationDonePredicate,
         Operation operation, long maxWaitSeconds) {
      if (operation == null) {
         return operation;
      }
      AtomicReference<Operation> operationReference = 
               Atomics.newReference(operation);
      retry(operationDonePredicate, maxWaitSeconds, 1, SECONDS)
               .apply(operationReference);
      return operationReference.get();
   }
   
   /**
    * Always close your service when you're done with it.
    */
   public void close() throws IOException {
      Closeables.close(computeService.getContext(), true);
   }
}
