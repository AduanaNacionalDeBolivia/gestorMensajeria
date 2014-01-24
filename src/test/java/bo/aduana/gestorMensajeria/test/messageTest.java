package bo.aduana.gestorMensajeria.test;

import java.util.logging.Logger;

import javax.inject.Inject;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import bo.aduana.gestorMensajeria.jms.MessageSender;
import bo.aduana.gestorMensajeria.jms.UserActionConsumer;
import bo.aduana.gestorMensajeria.service.MessageService;

@RunWith(Arquillian.class)
public class messageTest {

   @Inject
    Logger log;
   
   
   MessageService ms = new MessageService();
   
   @Before
   @Test
   public void sendMessages() throws Exception {
	   MessageSender ms = new MessageSender();
	   ms.sendMessages("{\"sender\" : \"Fabian Picon\",\"body\" : \"Esto es una prueba\",\"time\" : 100000,\"status\" : \"ok\",\"received\" : \"2014-01-23 17:19:57\",\"type\" : \"email\",\"recipient\" : \"Proyecto\"}");
   }
   
//   @Test
//   public void persistMessage() throws Exception {
//	   Message message = new Message("email", "Fabian Picon", "Fabian Picon", 100000L, "Esto es una prueba","readed");
//	   ms.save(message);
//   }
    
   @After
   @Test
   public void reciveMessages() throws Exception {
	   UserActionConsumer uac = new UserActionConsumer();
	   uac.processMessages();
   }
}
