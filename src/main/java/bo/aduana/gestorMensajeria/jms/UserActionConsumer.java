package bo.aduana.gestorMensajeria.jms;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONException;
import org.json.JSONObject;

import bo.aduana.gestorMensajeria.service.MessageService;
 
public class UserActionConsumer {
 
	private Properties properties;
    private int totalConsumedMessages = 0;
    
    MessageService messageService = new MessageService();
     
    public void processMessages() throws JMSException {
    	
    	loadProperties();
        final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(properties.getProperty("USER"), properties.getProperty("PASSWORD"), properties.getProperty("URL"));
        final Connection connection = connectionFactory.createConnection();
 
        connection.start();
 
        final Session session = connection.createSession(Boolean.parseBoolean(properties.getProperty("TRANSACTED_SESSION")), Session.AUTO_ACKNOWLEDGE);
        final Destination destination = session.createQueue(properties.getProperty("DESTINATION_QUEUE"));
        final MessageConsumer consumer = session.createConsumer(destination);
 
        processAllMessagesInQueue(consumer);
 
        consumer.close();
        session.close();
        connection.close();
 
        showProcessedResults();
    }
 
    private void processAllMessagesInQueue(MessageConsumer consumer) throws JMSException {
        Message message;
        while ((message = consumer.receive(Long.parseLong(properties.getProperty("TIMEOUT")))) != null) {
            proccessMessage(message);
        }
    }
 
    private void proccessMessage(Message message) throws JMSException {
    	 if (message instanceof TextMessage) {
             final TextMessage textMessage = (TextMessage) message;
             String text = textMessage.getText();
             JSONObject json = null;
			try {
				json = new JSONObject(text);
			} catch (JSONException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
             bo.aduana.gestorMensajeria.model.Message messageE;
 			try {
 				messageE = new bo.aduana.gestorMensajeria.model.Message(json.get("type").toString(), json.get("sender").toString(),json.get("recipient").toString(),Long.parseLong(json.get("time").toString()),json.get("body").toString(), json.get("status").toString());
 				 try {
 						messageService.save(messageE);
 					} catch (Exception e) {
 						// TODO Auto-generated catch block
 						e.printStackTrace();
 					}
 			} catch (NumberFormatException e1) {
 				// TODO Auto-generated catch block
 				e1.printStackTrace();
 			} catch (JSONException e1) {
 				// TODO Auto-generated catch block
 				e1.printStackTrace();
 			}
            
             totalConsumedMessages++;
         }
    }

 
    private void showProcessedResults() {
        System.out.println("Procesados un total de " + totalConsumedMessages + " mensajes");
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