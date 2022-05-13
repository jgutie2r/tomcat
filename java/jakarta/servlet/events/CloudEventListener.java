/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jakarta.servlet.events;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.cloudevents.CloudEvent;
import io.cloudevents.http.HttpMessageFactory;
import jakarta.servlet.GenericServlet;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

// Class created by Juan Gutiérrez-Aguado

/**
 * Provides an abstract class to be subclassed to create
 * a CloudEventListener suitable for consuming CloudEvents.
 * This class only deals with POST requests. <br/>
 * A subclass of <code>CloudEventListener</code> must override the method:
 * <ul>
 * <li>consumeEvent(CloudEventV1 ev, HttpServletRequest req, HttpServletResponse
 * resp)</li>
 * </ul>
 * <p>
 * The <code>service</code> method in this class verifies that
 * the request method is POST, retrieves the CloudEvent and
 * calls <code>consumeEvent</code> with the extracted event.
 * The <code>HttpServletRequest</code> object is passed to
 * <code>consumeEvent</code> just in case the user needs to
 * obtain some header. The <code>HttpServletResponse</code> is
 * passed to generate a response if needed.
 * <p>
 * Servlets typically run on multithreaded servers,
 * so be aware that a servlet must handle concurrent
 * requests and be careful to synchronize access to shared resources.
 * Shared resources include in-memory data such as
 * instance or class variables and external objects
 * such as files, database connections, and network
 * connections.
 * See the
 * <a href=
 * "http://java.sun.com/Series/Tutorial/java/threads/multithreaded.html">
 * Java Tutorial on Multithreaded Programming</a> for more
 * information on handling multiple threads in a Java program.
 */
public abstract class CloudEventListener extends GenericServlet {

   private static final long serialVersionUID = 1L;
   private static final String METHOD_POST = "POST";

   public CloudEventListener() {
      // NOOP
   }

   /**
    * Called by the server (via the <code>service</code> method)
    * to allow a servlet to handle a POST request with a CloudEvent.
    *
    * @param ev   the CloudEventV1 received
    *
    * @param req  the {@link HttpServletRequest} object that
    *             contains the request the client made of
    *             the servlet
    *
    *
    * @param resp the {@link HttpServletResponse} object that
    *             contains the response the servlet returns
    *             to the client
    *
    * @exception Exception if an error occurs
    *                      while the servlet is handling the
    *                      request
    *
    */

   public abstract void consumeEvent(CloudEvent ev, HttpServletRequest req, HttpServletResponse resp)
         throws ServletException, IOException;

   @Override
   public void init(ServletConfig config) throws ServletException {
      super.init(config);
   }

   /**
    * Reads the body of the request
    * 
    * @param req request
    * @return the body as an array of bytes
    * @throws IOException
    */
   private byte[] getBody(HttpServletRequest req) throws IOException {
      String tam = req.getHeader("Content-Length");
      byte[] body = null;
      InputStream in = req.getInputStream();
      if (tam != null) {
         int t = Integer.valueOf(tam);
         byte[] data = new byte[t];
         int leidos = in.read(data);
         body = Arrays.copyOfRange(data, 0, leidos);
      } else {
         ByteArrayOutputStream bout = new ByteArrayOutputStream();
         int c;
         while ((c = in.read()) != -1)
            bout.write(c);
         body = bout.toByteArray();
      }
      return body;
   }

   /**
    * Builds a cloud event from the request using cloud events http basic classes
    * 
    * @param req request
    * @return the cloud event
    * @throws IOException
    */
   private CloudEvent readEvent(HttpServletRequest req) throws IOException {
      if (req.getHeader("Content-Type").contains("content-type: application/cloudevents+json")) {
         // Not implemented: read the body and convert from JSON to CloudEvent
         // TODO
         return null;

      } else {
         Consumer<BiConsumer<String, String>> forEachHeader = processHeader -> {
            Enumeration<String> headerNames = req.getHeaderNames();
            while (headerNames.hasMoreElements()) {
               String name = headerNames.nextElement();
               processHeader.accept(name, req.getHeader(name));
            }
         };
         byte[] body = getBody(req);
         return HttpMessageFactory.createReader(forEachHeader, body).toEvent();
      }
   }

   /**
    * Checks if it is a POST request, obtains the cloud event and calls
    * consumeEvent with the event, the request and the response
    * 
    * @param req  request
    * @param resp response
    * @throws ServletException
    * @throws IOException
    */
   protected void service(HttpServletRequest req, HttpServletResponse resp)
         throws ServletException, IOException {

      String method = req.getMethod();

      if (method.equals(METHOD_POST)) {
         try {
            CloudEvent cev = readEvent(req);
            consumeEvent(cev, req, resp);
         } catch (IOException ex) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Empty body in request");
         }
      } else {
         // As this is intended to consume cloud events and post requests
         // other methods are not allowed
         resp.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED, "Method not implemented");
      }
   }

   @Override
   public void service(ServletRequest req, ServletResponse res)
         throws ServletException, IOException {

      HttpServletRequest request;
      HttpServletResponse response;

      try {
         request = (HttpServletRequest) req;
         response = (HttpServletResponse) res;
      } catch (ClassCastException e) {
         throw new ServletException("Not an HTTP request");
      }
      service(request, response);
   }

   /**
    * Sends a cloud event to the client
    * 
    * @param ev   cloud event to send
    * @param resp reference to object that encapsulates the response
    */
   public void sendCloudEvent(CloudEvent ev, HttpServletResponse resp) throws IOException {
      HttpMessageFactory.createWriter(
            resp::addHeader,
            body -> {
               try {
                  if (body != null) {
                     try (OutputStream outputStream = resp.getOutputStream()) {
                        resp.addHeader("Content-Length", "" + body.length);
                        resp.setStatus(HttpServletResponse.SC_OK);
                        outputStream.write(body);
                        outputStream.flush();
                        outputStream.close();                        
                     }
                  } else {
                     resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
                  }

               } catch (IOException e) {
                  throw new UncheckedIOException(e);
               }
            }).writeBinary(ev);

   }
}