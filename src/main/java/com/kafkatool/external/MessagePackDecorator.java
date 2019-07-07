package com.kafkatool.external;

import org.msgpack.MessagePack;

import java.io.IOException;
import java.util.Map;

public class MessagePackDecorator implements ICustomMessageDecorator {
  private MessagePack messagePack;

  public MessagePackDecorator() {
    messagePack = new MessagePack();
  }

  @Override
  public String getDisplayName() { return "MessagePack"; }

  @Override
  public String decorate(String zookeeperHost, String brokerHost, String topic, long partitionId, long offset, byte[] msg, Map<String, String> reserved) {
    try {
      return messagePack.read(msg).toString();
    } catch (IOException e) {
      return bytesToHex(msg);
    }
  }

  public static String bytesToHex(byte[] bytes) {
    StringBuffer sb = new StringBuffer();
    for(int i = 0; i < bytes.length; i++) {
      String hex = Integer.toHexString(bytes[i] & 0xFF);
      if(hex.length() < 2){
        sb.append(0);
      }
      sb.append(hex);
    }
    return sb.toString();
  }
}
