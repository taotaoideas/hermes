package com.ctrip.hermes.rest.resource;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/test")
public class TestResource {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String get2(){
		return "Hermes Server is OK!";
	}

}
