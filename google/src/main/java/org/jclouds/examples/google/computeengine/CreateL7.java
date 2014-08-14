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
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.compute.predicates.NodePredicates;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApi;
import org.jclouds.googlecomputeengine.config.UserProject;
import org.jclouds.googlecomputeengine.domain.BackendService.Backend;
import org.jclouds.googlecomputeengine.domain.Firewall.Rule;
import org.jclouds.googlecomputeengine.domain.ForwardingRule;
import org.jclouds.googlecomputeengine.domain.Instance;
import org.jclouds.googlecomputeengine.domain.Operation;
import org.jclouds.googlecomputeengine.domain.ResourceView;
import org.jclouds.googlecomputeengine.domain.UrlMap.HostRule;
import org.jclouds.googlecomputeengine.domain.UrlMap.PathMatcher;
import org.jclouds.googlecomputeengine.domain.UrlMap.PathRule;
import org.jclouds.googlecomputeengine.options.BackendServiceOptions;
import org.jclouds.googlecomputeengine.options.FirewallOptions;
import org.jclouds.googlecomputeengine.options.ForwardingRuleOptions;
import org.jclouds.googlecomputeengine.options.ResourceViewOptions;
import org.jclouds.googlecomputeengine.options.UrlMapOptions;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.net.domain.IpProtocol;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;
import org.jclouds.sshj.config.SshjSshClientModule;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Atomics;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

/**
 * This example creates a 3 Debian Wheezy n1-standard-1 instances, a firewall
 * for http traffic on port 80, 3 resource views, 3 backend services, a url map,
 * a target http proxy, and a global forwarding rule. The resource views,
 * backend services, url map, target http proxy, and global forwarding rule
 * enable Layer 7 load balancing on Google Compute Engine.
 * 
 * This example file is meant to mirror the tutorial at
 * <a href="https://developers.google.com/compute/docs/load-balancing/http/content-based-example" />
 */
public class CreateL7 implements Closeable {
   /**
    * To get a service account and its private key see [TODO: write some
    * documentation on the website and put a link to it]
    *
    * The first argument (args[0]) is your service account email address
    *    (https://developers.google.com/console/help/new/#serviceaccounts).
    * The second argument (args[1]) is a path to your service account private
    *    key PEM file without a password. It is used for server-to-server
    *    interactions
    *    (https://developers.google.com/console/help/new/#serviceaccounts).
    *    The key is not transmitted anywhere.
    * Before running this example you must have an http health check on GCE
    *    with the name "jclouds-l7-example-health-check".
    *
    * Example:
    *
    * java org.jclouds.examples.google.computeengine.CreateServer \
    *    somecrypticname@developer.gserviceaccount.com \
    *    /home/planetnik/Work/Cloud/OSS/certificate/gcp-oss.pem
    *    
    *    Note that no ssh credentials are needed for this example. Jclouds
    *    automatically creates a jclouds user with an ssh key when nodes are
    *    provisioned. However, this also means that project ssh keys stored in
    *    Google Compute Engine are never pushed over to jclouds provisioned
    *    instances. In order to ssh into these instances with your regular
    *    credentials you would have to go to the Developers Console and either
    *    add your default credentials to the instance's ssh keys or delete all
    *    instance specific ssh keys.
    *    
    *    Note that it may take several minutes for the load balancer to start
    *    directing traffic after the program completes.
    */
   
   private static final String HEALTH_CHECK = L7_GROUP_NAME + "-health-check";
   
   private final ComputeService computeService;
   private final GoogleComputeEngineApi api;
   private static String project;
   private Predicate<AtomicReference<Operation>> globalOperationDonePredicate;
   
      
      public CreateL7(String serviceAccountEmailAddress,
               String serviceAccountKey) {
         ComputeServiceContext context = ContextBuilder
               .newBuilder(PROVIDER)
               .credentials(serviceAccountEmailAddress, serviceAccountKey)
               .modules(ImmutableSet.<Module> of(new SshjSshClientModule(),
                                                 new SLF4JLoggingModule()))
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
         
         CreateL7 createL7 = new CreateL7(serviceAccountEmailAddress,
                                          serviceAccountKey);
         
         try {
            System.out.println("Creating nodes");
            Set<? extends NodeMetadata> nodes =
                  createL7.createNodes(L7_GROUP_NAME, NUM_INSTANCES, ZONE);
            
            if (nodes.size() < NUM_INSTANCES) {
               throw new RuntimeException("Unable to successfully provision "
                     + NUM_INSTANCES + " nodes. Please try again");
            }
            
            // Grab some information about tags / networks for later use,
            // but first get an ordered set of our instances so that we can do
            // different things for different instances.
            NodeMetadata[] nodeArray = nodes.toArray(
                  new NodeMetadata[NUM_INSTANCES]);
            Instance first = createL7.getInstance(ZONE,
                  nodeArray[0].getHostname());
            URI network = createL7.getNetwork(first);
            // These instances only have 1 tag, so just getting the first one
            // is fine.
            String tag = createL7.getTag(first);
            System.out.println("Installing apache");
            createL7.installApache(L7_GROUP_NAME);
            System.out.println("Setting up www server");
            createL7.installWww(nodeArray[0]);
            System.out.println("Setting up video server");
            createL7.installVideo(nodeArray[1]);
            System.out.println("Setting up static server");
            createL7.installStatic(nodeArray[2]);
            
            System.out.println("Creating firewall");
            createL7.createFirewall(L7_GROUP_NAME + "-firewall", network, tag);
            
            System.out.println("Creating resource views");
            ResourceView[] resourceViews = new ResourceView[NUM_INSTANCES];
            int i = 0;
            for (NodeMetadata n : nodeArray) {
               resourceViews[i] = createL7.createResourceView(ZONE,
                     L7_GROUP_NAME + "-resource-view-" + i,
                     ImmutableSet.<URI>of(n.getUri()));
               i++;
            }
            
            System.out.println("Creating backend services");
            URI[] backendServices = new URI[NUM_INSTANCES];
            i = 0;
            for (ResourceView r : resourceViews) {
               backendServices[i] = createL7.createBackendService(
                        L7_GROUP_NAME + "-backend-service-" + i,
                        HEALTH_CHECK, r.getSelfLink());
               i++;
            }
            
            System.out.println("Creating url map");
            URI urlMap = createL7.createUrlMap(L7_GROUP_NAME + "-url-map",
                  backendServices);
            
            System.out.println("Creating target http proxy");
            URI targetHttpProxy = createL7.createTargetHttpProxy(
                  L7_GROUP_NAME + "-target-http-proxy", urlMap);
            
            System.out.println("Creating global forwarding rule");
            createL7.createForwardingRule(L7_GROUP_NAME + "-forwarding-rule",
                  targetHttpProxy);
            
            System.out.println("Created global forwarding rule with IP "
                  + createL7.getForwardingRule(L7_GROUP_NAME + "-forwarding-rule")
                     .getIpAddress().get());
         } catch (Exception e) {
            e.printStackTrace();
         } finally {
            createL7.close();
         }
      }
      
      /**
       * Returns an Instance object of the instance with the given name if the
       * instance exists.
       * 
       * @param zone    the zone the instance lives in.
       * @param name    the name of the instance.
       * @return        an Instance object if the instance exists, else null.
       */
      private Instance getInstance(String zone, String name) {
         return api.getInstanceApiForProject(project).getInZone(zone, name);
      }
      
      /**
       * Returns the URI of the network the given instance is on.
       * 
       * @param instance   the instance whose network to get.
       * @return           the URI of the network the instance is on.
       */
      private URI getNetwork(Instance instance) {
         return instance.getNetworkInterfaces().iterator().next().getNetwork();
      }
      
      /**
       * Returns the first tag of the given instance.
       * 
       * @param instance   the instance whose tag to get.
       * @return           the first retrieved tag of the instance.
       */
      private String getTag(Instance instance) {
         return instance.getTags().getItems().iterator().next();
      }
      
      /**
       * Returns a ForwardingRule object of the given name if the forwarding
       * rule exists.
       * 
       * @param name    the name of the forwarding rule to get.
       * @return        a ForwardingRule object if a forwarding rule with the
       *       given name exists on GCE, else null.
       */
      private ForwardingRule getForwardingRule(String name) {
         return api.getForwardingRuleApiForProject(project).get(name);
      }
      
      /**
       * Creates a number of nodes on Google Compute Engine.
       * 
       * @param group                  the base name of the instances to be
       *       created.
       * @param numInstances           the number of instances to create.
       * @param zone                   the zone the instances should reside in.
       * @return                       the metadata of the nodes that were
       *       successfully created.
       * @throws RunNodesException
       */
      private Set<? extends NodeMetadata> createNodes(String group,
            int numInstances, String zone) 
            throws RunNodesException {
         Template template = computeService.templateBuilder()
               .smallest()
               .osFamily(OsFamily.DEBIAN)
               .locationId(zone)
               .build();
         Set<? extends NodeMetadata> nodes = computeService
               .createNodesInGroup(group, numInstances, template);
         return nodes;
      }
      
      /**
       * Installs apache2 on the nodes in the given group.
       * 
       * @param group                        the group name to run the script
       *       on.
       * @throws RunScriptOnNodesException
       * @throws IOException
       */
      private void installApache(String group)
                        throws RunScriptOnNodesException, IOException {
         new RunScriptOptions.Builder();
         RunScriptOptions options = RunScriptOptions.Builder
               .blockOnComplete(true)
               .runAsRoot(true);
         
         Statement script = Statements.newStatementList(
               Statements.exec("apt-get update"),
               Statements.exec("apt-get install apache2 -y"));
         
         computeService.runScriptOnNodesMatching(
               NodePredicates.inGroup(group), script, options);
      }
      
      /**
       * Runs a script on the given node.
       * 
       * @param node    the metadata of the node to run the script on.
       * @param script  the script to run.
       */
      private void runScriptOnNode(NodeMetadata node, Statement script) {
         new RunScriptOptions.Builder();
         RunScriptOptions options = RunScriptOptions.Builder
               .blockOnComplete(true)
               .runAsRoot(true);
         
         computeService.runScriptOnNode(node.getId(), script, options);
      }
      
      /**
       * Generates the index.html page for the www node in this demo.
       * 
       * @param node          the metadata of the node to generate the html
       *       file on.
       * @throws IOException
       */
      private void installWww(NodeMetadata node) throws IOException {
         String file = CharStreams.toString(new InputStreamReader(
               CreateL7.class.getResourceAsStream("index.html")));
         
         Statement script = Statements.newStatementList(
               Statements.exec("cat > /var/www/index.html <<END{lf}"
                     + file + "{lf}END"));
         
         runScriptOnNode(node, script);
      }
      
      /**
       * Generates the index.html page for the static node in this demo.
       * 
       * @param node          the metadata of the node to generate the html
       *       file on.
       * @throws IOException
       */
      private void installStatic(NodeMetadata node) throws IOException {
         String file = CharStreams.toString(new InputStreamReader(
               CreateL7.class.getResourceAsStream("index.html")));
         
         Statement script = Statements.newStatementList(
               Statements.exec("mkdir /var/www/static"),
               Statements.exec("cat > /var/www/index.html <<END{lf}"
                     + file + "{lf}END"),
               Statements.exec("cat > /var/www/static/index.html <<END{lf}"
                     + file + "{lf}END"));
         
         runScriptOnNode(node, script);
      }
      
      /**
       * Generates the index.html page for the video node in this demo.
       * 
       * @param node          the metadata of the node to generate the html
       *       file on.
       * @throws IOException
       */
      private void installVideo(NodeMetadata node) throws IOException {
         String file = CharStreams.toString(new InputStreamReader(
               CreateL7.class.getResourceAsStream("index.html")));
         
         Statement script = Statements.newStatementList(
               Statements.exec("mkdir /var/www/video"),
               Statements.exec("cat > /var/www/index.html <<END{lf}"
                     + file + "{lf}END"),
               Statements.exec("cat > /var/www/video/index.html <<END{lf}"
                     + file + "{lf}END"));
         
         runScriptOnNode(node, script);
      }
      
      /**
       * Creates a basic firewall rule that allows traffic from any IP on port
       * 80 to instances with the given tag.
       * 
       * @param name       the name of this firewall.
       * @param network    the network this firewall exists on.
       * @param tag        the tag of the instances this firewall applies to.
       * @return           the selfLink of this firewall.
       */
      private URI createFirewall(String name, URI network, String tag) {
         Rule rule = Rule.builder()
               .addPort(80)
               .IpProtocol(IpProtocol.TCP)
               .build();
         
         FirewallOptions options = new FirewallOptions();
         options.network(network)
               .addAllowedRule(rule)
               .addSourceRange("0.0.0.0/0")
               .addTargetTag(tag);
         
         return waitGlobalOperationDone(api.getFirewallApiForProject(project)
                  .createInNetwork(name, network, options),
               MAX_WAIT);
      }
      
      /**
       * Creates a basic resource view.
       * 
       * @param zone       the zone this resource view will be in.
       * @param name       the name of this resource view.
       * @param members    the instances in this resource view.
       * @return           a ResourceView object representing this resource
       *       view.
       */
      private ResourceView createResourceView(String zone, String name,
               Set<URI> members) {
         ResourceViewOptions resourceViewOptions = new ResourceViewOptions();
         resourceViewOptions.zone(zone)
               .members(members);
         
         return api.getResourceViewApiForProject(project).createInZone(zone,
               name, resourceViewOptions);
      }
      
      /**
       * Creates a basic backend service.
       * 
       * @param name          the name of this backend service.
       * @param healthCheck   the name of the health check this backend service
       *       should use.
       * @param resourceView  the selfLink of the resource view this backend
       *       service will have as its backend.
       * @return              the selfLink of this backend service.
       */
      private URI createBackendService(String name, String healthCheck,
            URI resourceView) {
         Backend backend = Backend.builder()
               .group(resourceView)
               .build();
         
         BackendServiceOptions options = new BackendServiceOptions();
         options.addHealthCheck(URI.create("https://www."
                  + "googleapis.com/compute/v1/projects/" + project
                  + "/global/healthChecks/" + healthCheck))
               .addBackend(backend);
         
         return waitGlobalOperationDone(api
               .getBackendServiceApiForProject(project).create(name, options),
                  MAX_WAIT);
      }
      
      /**
       * Creates a basic url map.
       * 
       * @param name             the name of this url map.
       * @param backendServices  the selfLinks of the backend services this
       *       url map will link to.
       * @return                 the selfLink of this url map.
       */
      private URI createUrlMap(String name, URI[] backendServices) {
         HostRule hostRule = HostRule.builder()
               .addHost("*")
               .pathMatcher("pathmap")
               .build();
         
         PathRule videoPathRule = PathRule.builder()
               .paths(ImmutableSet.<String>of("/video", "/video/*"))
               .service(backendServices[1])
               .build();
         PathRule staticPathRule = PathRule.builder()
               .paths(ImmutableSet.<String>of("/static", "/static/*"))
               .service(backendServices[2])
               .build();
         
         PathMatcher pathMatcher = PathMatcher.builder()
               .name("pathmap")
               .defaultService(backendServices[0])
               .pathRules(ImmutableSet.<PathRule>of(videoPathRule,
                     staticPathRule))
               .build();
         
         UrlMapOptions options = new UrlMapOptions();
         options.defaultService(backendServices[0])
               .pathMatchers(ImmutableSet.<PathMatcher>of(pathMatcher))
               .hostRules(ImmutableSet.<HostRule>of(hostRule));
         
         return waitGlobalOperationDone(api.getUrlMapApiForProject(project)
                  .create(name, options),
               MAX_WAIT);
      }
      
      /**
       * Creates a basic target http proxy.
       * 
       * @param name       the name of this target http proxy.
       * @param url map    the self link of the url map this target http proxy
       *       will link to.
       * @return           the selfLink of this target http proxy.
       */
      private URI createTargetHttpProxy(String name, URI urlMap) {
         return waitGlobalOperationDone(api
                     .getTargetHttpProxyApiForProject(project).create(name,
                           urlMap),
                  MAX_WAIT);
      }
      
      /**
       * Creates a basic global forwarding rule.
       * 
       * @param name       the name of this forwarding rule.
       * @param url map    the self link of the target http proxy this
       *       forwarding rule will link to.
       * @return           the selfLink of this forwarding rule.
       */
      private URI createForwardingRule(String name, URI targetHttpProxy) {
         ForwardingRuleOptions options = new ForwardingRuleOptions();
         options.ipProtocol("TCP")
               .portRange("80")
               .target(targetHttpProxy);
         
         return waitGlobalOperationDone(api
                  .getForwardingRuleApiForProject(project).create(name,
                        options),
                  MAX_WAIT);
      }
      
      /**
       * Waits until the given global operation is completed.
       * 
       * @param operation        the operation to wait for.
       * @param maxWaitSeconds   the maximum number of seconds to wait.
       * @return                 the self link of the resource the operation
       *       was performed on.
       */
      private URI waitGlobalOperationDone(Operation operation,
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
      private static URI waitOperationDone(
               Predicate<AtomicReference<Operation>> operationDonePredicate,
               Operation operation, long maxWaitSeconds) {
         if (operation == null) {
            return null;
         }
         AtomicReference<Operation> operationReference =
               Atomics.newReference(operation);
         retry(operationDonePredicate, maxWaitSeconds, 1, SECONDS)
               .apply(operationReference);
         return operationReference.get().getTargetLink();
      }
      
      /**
       * Always close your service when you're done with it.
       */
      public void close() throws IOException {
         Closeables.close(computeService.getContext(), true);
      }
}