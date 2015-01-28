package com.ctrip.hermes.container.console.home;

public enum JspFile {
	VIEW("/jsp/console/home.jsp"),

	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
