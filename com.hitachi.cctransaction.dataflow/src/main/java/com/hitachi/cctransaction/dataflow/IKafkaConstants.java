package com.hitachi.cctransaction.dataflow;

public interface IKafkaConstants {
	//172.19.2.122:6667
   // public static String KAFKA_BROKERS = "localhost:9092";
    public static String KAFKA_BROKERS = "172.19.2.122:6667";
    public static Integer MESSAGE_COUNT=10;
    public static String CLIENT_ID="client1";
    public static String TOPIC_NAME="cc_trans_raw";
    public static String GROUP_ID_CONFIG="consumerGroup1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
    public static String OFFSET_RESET_LATEST="latest";
    public static String OFFSET_RESET_EARLIER="earliest";
    public static Integer MAX_POLL_RECORDS=1;
}
