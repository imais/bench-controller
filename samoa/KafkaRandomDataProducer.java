import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.streams.generators.RandomTreeGenerator;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class KafkaRandomDataProducer extends Thread {

    private static final String DEFAULT_TOPIC = "test";
    private static final String DEFAULT_BROKERS = "localhost:9092";
    private static final int DEFAULT_NUM_CLASSES = 2;
    private static final int DEFAULT_NUM_NOMINALS = 5;
    private static final int DEFAULT_NUM_NUMERICS = 5;

    private final KafkaProducer<String, String> producer_;
    private String topic_;
    private RandomTreeGenerator stream_;

    public KafkaRandomDataProducer(String brokers, String topic, 
                                   int numClasses, int numNominals, int numNumerics) {
        System.out.println("Brokers: " + brokers);
        System.out.println("Topic: " + topic);
        System.out.println("Num classes: " + numClasses);
        System.out.println("Num nominals: " + numNominals);
        System.out.println("Num numerics: " + numNumerics);

        topic_ = topic;
        stream_ = new RandomTreeGenerator();
        stream_.numClassesOption.setValue(numClasses);
        stream_.numNominalsOption.setValue(numNominals);
        stream_.numNumericsOption.setValue(numNumerics);

        if (stream_ instanceof AbstractOptionHandler) {
            ((AbstractOptionHandler) (stream_)).prepareForUse();
        }        

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "RandProducer");
        props.put("key.serializer",
                  "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                  "org.apache.kafka.common.serialization.StringSerializer");
        producer_ = new KafkaProducer<String, String>(props);
    }

    public void sendMessage(String key, String value) {
        producer_.send(new ProducerRecord<String, String>(topic_, key, value));
        // try {
        //     Thread.sleep(1000);
        // } catch (Exception ex) {
        //     System.err.println(ex);
        // }
        // System.out.println("Sent message: (" + key + ", " + value + ")");
    }

    public String createRandomData() {
        Instance inst = stream_.nextInstance().getData();
        return inst.toString() + "," + inst.classValue();
    }


    public void run() {
        int msgCount = 0;
        while (true) {
            String msg = createRandomData();
            sendMessage(msgCount + "", msg);
            msgCount++;
        }        
    }

    public static void main(String [] args){
        Options opts = new Options();
        opts.addOption("b", "brokers", true, "Kafka brokers.");
        opts.addOption("t", "topic", true, "Kafka topic to consume.");
        opts.addOption("c", "num_classes", true, "Number of classes to generate.");
        opts.addOption("n", "num_nominals", true, "Number of nominal attributes to generate.");
        opts.addOption("m", "num_numerics", true, "Number of numeric attributes to generate.");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(opts, args);
        } catch (ParseException ex) {
            System.err.println(ex);
        }

        String brokers = cmd.getOptionValue("brokers");
        brokers = (brokers == null) ? DEFAULT_BROKERS : brokers;
        String topic = cmd.getOptionValue("topic");
        topic = (topic == null) ? DEFAULT_TOPIC : topic;
        String temp = cmd.getOptionValue("num_classes");
        int numClasses = (temp == null) ? DEFAULT_NUM_CLASSES : Integer.parseInt(temp);
        temp = cmd.getOptionValue("num_nominals");
        int numNominals = (temp == null) ? DEFAULT_NUM_NOMINALS : Integer.parseInt(temp);
        temp = cmd.getOptionValue("num_numerics");
        int numNumerics = (temp == null) ? DEFAULT_NUM_NUMERICS : Integer.parseInt(temp);

        KafkaRandomDataProducer randomDataProducer = new KafkaRandomDataProducer(
            brokers, topic, numClasses, numNominals, numNumerics);
        randomDataProducer.run();
    }
}

