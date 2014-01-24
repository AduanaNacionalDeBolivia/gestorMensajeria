package bo.aduana.gestorMensajeria.jms;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
 
public class MessageSender {
 
	private Properties properties;
	
    public void sendMessages(String message) throws JMSException {
    	
    	loadProperties();
        final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(properties.getProperty("USER"), properties.getProperty("PASSWORD"), properties.getProperty("URL"));
        Connection connection = connectionFactory.createConnection();
        connection.start();
 
        final Session session = connection.createSession(Boolean.parseBoolean(properties.getProperty("TRANSACTED_SESSION")), Session.AUTO_ACKNOWLEDGE);
        final Destination destination = session.createQueue(properties.getProperty("DESTINATION_QUEUE"));
 
        final MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
 
        sendMessages(session, producer, message);
        session.commit();
 
        session.close();
        connection.close();
 
        System.out.println("Mensajes enviados correctamente");
    }
 
    private void sendMessages(Session session, MessageProducer producer, String message) throws JMSException {
    	final MessageSender messageSender = new MessageSender();
        messageSender.sendMessage(message, session, producer);
    }
 
    private void sendMessage(String message, Session session, MessageProducer producer) throws JMSException {
        final TextMessage textMessage = session.createTextMessage(message);
        producer.send(textMessage);
    }
    
    private void loadProperties(){
    	
    	try {
			properties = new Properties();
			properties.load(this.getClass().getClassLoader().getResourceAsStream("activeMQ.properties"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }

}