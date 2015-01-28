package com.ctrip.hermes.broker.console.home;

import com.ctrip.hermes.broker.console.ConsolePage;
import org.unidal.web.mvc.ViewModel;

public class Model extends ViewModel<ConsolePage, Action, Context> {
	public Model(Context ctx) {
		super(ctx);
	}

	@Override
	public Action getDefaultAction() {
		return Action.VIEW;
	}
}
