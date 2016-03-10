package kafka;

/**
 * Created by Hadhami on 08/03/2016.
 */
public class Main {
    public static void main(String[] args)throws Exception {
        KafkaHelper helper = new KafkaHelper();
        //helper.createTopic("topic_2",2,2);
        helper.createProducer();
        helper.writeMessage("haha ha", "kafka");
        helper.closeProducer();

        //Consumer<String, String> cons = helper.createConsumer("new");
        //Map<String, ConsumerRecords<String, String>> result= helper.getMessages();
        //helper.closeConsumer(cons);




    }
}
