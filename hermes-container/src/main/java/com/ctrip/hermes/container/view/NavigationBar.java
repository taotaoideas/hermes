package com.ctrip.hermes.container.view;

import com.ctrip.hermes.container.console.ConsolePage;
import org.unidal.web.mvc.Page;

public class NavigationBar {
   public Page[] getVisiblePages() {
      return new Page[] {
   
      ConsolePage.HOME

		};
   }
}
