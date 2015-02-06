package com.ctrip.hermes.netty;

public class RemotingCmdHelper {
    public static RemotingCmd buildRequestCmd(byte[] body) {
        RemotingCmd cmd = new RemotingCmd();
        cmd.setType(RemotingCmd.REQUEST_TYPE);
        cmd.setBody(body);

        return cmd;
    }

    public static RemotingCmd buildResponseCmd(byte[] body) {
        RemotingCmd cmd = new RemotingCmd();
        cmd.setType(RemotingCmd.REQUEST_TYPE);
        cmd.setBody(body);

        return cmd;
    }

    public static boolean isRequestCmd(RemotingCmd cmd) {
        return cmd.getType() == RemotingCmd.REQUEST_TYPE;
    }

    public static boolean isResponseCmd(RemotingCmd cmd) {
        return cmd.getType() == RemotingCmd.RESPONSE_TYPE;
    }
}
