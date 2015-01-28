package com.ctrip.hermes.container.console;

import org.unidal.web.mvc.AbstractModule;
import org.unidal.web.mvc.annotation.ModuleMeta;
import org.unidal.web.mvc.annotation.ModulePagesMeta;

@ModuleMeta(name = "console", defaultInboundAction = "home", defaultTransition = "default", defaultErrorAction = "default")
@ModulePagesMeta({

com.ctrip.hermes.container.console.home.Handler.class
})
public class ConsoleModule extends AbstractModule {

}
