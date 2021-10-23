package org.apache.pinot.common.messages;

import java.util.UUID;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;

public class SegmentDeleteMessage extends Message {
  public static final String DELETE_SEGMENT_MSG_SUB_TYPE = "DELETE_SEGMENT";

  private static final String TABLE_NAME_KEY = "tableName";
  private static final String SEGMENT_NAME_KEY = "segmentName";

  /**
   * Constructor for the sender.
   */
  public SegmentDeleteMessage(String tableNameWithType, String segmentName) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(DELETE_SEGMENT_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
    // Set the Pinot specific fields
    // NOTE: DO NOT use Helix fields "RESOURCE_NAME" and "PARTITION_NAME" for them because these 2 fields can be
    // overridden by Helix while sending the message
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME_KEY, tableNameWithType);
    znRecord.setSimpleField(SEGMENT_NAME_KEY, segmentName);
  }

  /**
   * Constructor for the receiver.
   *
   * @param message The incoming message that has been received from helix.
   * @throws IllegalArgumentException if the message is not of right sub-type
   */
  public SegmentDeleteMessage(Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(DELETE_SEGMENT_MSG_SUB_TYPE)) {
      throw new IllegalArgumentException("Invalid message subtype:" + message.getMsgSubType());
    }
  }

  public String getTableNameWithType() {
    return getRecord().getSimpleField(TABLE_NAME_KEY);
  }

  public String getSegmentName() {
    return getRecord().getSimpleField(SEGMENT_NAME_KEY);
  }
}
