import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Objects;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jms.core.JmsTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LargeMessageTest {

    public static final String TOPIC_NAME = "TEST_TOPIC";

    private static EmbeddedActiveMQExtension embeddedActiveMQResource;
    private static JmsTemplate jmsTemplate;

    @BeforeAll
    public static void setupEnvironment() throws Exception {
        embeddedActiveMQResource = new EmbeddedActiveMQExtension();
        ServerLocator serverLocator = ActiveMQClient.createServerLocator(embeddedActiveMQResource.getVmURL());
        ActiveMQJMSConnectionFactory connectionFactory = new ActiveMQJMSConnectionFactory(serverLocator);
        jmsTemplate = new JmsTemplate(connectionFactory);
    }

    @BeforeEach
    void startServer(){
        embeddedActiveMQResource.start();
    }
    @AfterEach
    void stopServer(){
        embeddedActiveMQResource.stop();
    }

    @Test
    void testInputStream() throws URISyntaxException, ActiveMQException, IOException {
        File file = new File(Objects.requireNonNull(getClass().getResource("/testFile.txt")).toURI());
        String content = Files.readString(file.toPath());

        InputStream inputStream = new FileInputStream(file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

        jmsTemplate.send(LargeMessageTest.TOPIC_NAME, session -> {
            javax.jms.Message msg = session.createBytesMessage();

            msg.setObjectProperty("JMS_AMQ_InputStream", bufferedInputStream);
            return msg;
        });

        ClientMessage receivedMessage = embeddedActiveMQResource.receiveMessage(TOPIC_NAME);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BufferedOutputStream bufferedOutput = new BufferedOutputStream(byteArrayOutputStream);
        receivedMessage.saveToOutputStream(bufferedOutput);

        String readContent = byteArrayOutputStream.toString();

        assertEquals(content, readContent);
    }
}
