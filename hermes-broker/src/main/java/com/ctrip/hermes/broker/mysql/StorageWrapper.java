package com.ctrip.hermes.broker.mysql;

import com.ctrip.hermes.message.Message;

/**
 * wrap basic action to mysql.
 */
public class StorageWrapper {

   /* private String topic;

    public StorageWrapper(String topic) {
        this.topic = topic;
        init();
    }


    *//**
     * init and get connection
      *//*
    private void init() {

    }

    public void write(Message msg) {
        // TODO: send msg into mysql: msg_high_[topic] or msg_low_[topic]
    }

    public Message readBySingle(int batchSize, MessagePriority priority) {

    }

    public Message readByGroup(int batchSize, MessagePriority priority, String groupId) {
        // TODO: 1. get msg from msg_[priority]_[topic]
        //       2. insert new offset into msg_offset_[topic]


    }

    public void updateMessageOffset(int offset, String groupId) {

    }

    public void writeResend(int start, int end, String groupId) {

    }

    public void getResendMessage(int batchSize, MessagePriority priority, String groupId) {
        // TODO: 0. get offset of resend from resend_offset_[topic]
        //       1. get offsets of msgs from resend_[priority]_[topic]
        //       2. get msgs from msg_[priority]_[topic]

    }

    public void updateResendOffset(int offset, String groupId) {

    }
*/

    // consumer.consume()
    // consumer.resend()

}
