package com.ctrip.hermes.broker.view;

import com.ctrip.hermes.broker.console.ConsolePage;
import org.unidal.web.mvc.Page;

public class NavigationBar {
   public Page[] getVisiblePages() {
      return new Page[] {
   
      ConsolePage.HOME

		};
   }
}
