package com.ctrip.hermes.meta.rest;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;

public class CharsetResponseFilter implements ContainerResponseFilter {
   private static final String APPEND_CHARSET_UTF8 = "; charset=UTF-8";
   private static final String CHARSET_UTF8 = "charset=UTF-8";
   
   @Override
   public void filter(ContainerRequestContext requestContext,
           ContainerResponseContext responseContext) throws IOException {
       MultivaluedMap<String, Object> headers = responseContext.getHeaders();
       List<Object> contentTypes = headers.remove(HttpHeaders.CONTENT_TYPE);
       if (contentTypes != null && !contentTypes.isEmpty()) {
           String contentType = contentTypes.get(0).toString();
           String sanitizedContentType = contentType + APPEND_CHARSET_UTF8;
           headers.add(HttpHeaders.CONTENT_TYPE, sanitizedContentType);
       } else {
           headers.add(HttpHeaders.CONTENT_TYPE, CHARSET_UTF8);
       }
   }
}

