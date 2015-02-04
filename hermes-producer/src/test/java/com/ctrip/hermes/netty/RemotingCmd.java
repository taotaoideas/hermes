package com.ctrip.hermes.netty;

public class RemotingCmd {

    public static final int REQUEST_TYPE = 0;
    public static final int RESPONSE_TYPE = 1;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    private int type;

    private byte[] body;
}

