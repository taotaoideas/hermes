package com.ctrip.hermes.container.console.home;

import com.ctrip.hermes.container.console.ConsolePage;
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
