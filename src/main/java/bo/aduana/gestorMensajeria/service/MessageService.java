package bo.aduana.gestorMensajeria.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Named;

import org.bson.types.ObjectId;

import bo.aduana.gestorMensajeria.model.Message;
import bo.aduana.gestorMensajeria.mongodb.MongoConectionManager;

@Named("messageService")
public class MessageService {
	
	private MongoConectionManager mcm = new MongoConectionManager();

	public ObjectId save(Message message) throws Exception{
		mcm.persist(message.toJsonStr());
		if(mcm.hasError())
			throw new Exception(mcm.getLastErrorMsg());
		return mcm.getLastId();
	}


	public List<Message> findAll() {
		return mcm.findAll();
	}


	public List<Message> findBy(Map<String,Object> filter) {
		List<Message> msgs = null;
		Map<String,Object> filter2 = new HashMap<String,Object>();
		if(filter!=null){
			for (String key : filter.keySet()) {			
				filter2.put("msg."+key,filter.get(key));
			}
		}
		msgs = mcm.find(filter2);
		return msgs;
	}
	
	public List<Message> findBy(Map<String,Object> filter, int pageNumber, int nPerPage) {
		List<Message> msgs = null;
		Map<String,Object> filter2 = new HashMap<String,Object>();
		if(filter!=null){
			for (String key : filter.keySet()) {
				filter2.put("msg."+key,filter.get(key));
			}
		}
		msgs = mcm.findPaginate(filter2, pageNumber, nPerPage);
		return msgs;
	}	
	
	public int countQuery(Map<String,Object> filter) {
		Map<String,Object> filter2 = new HashMap<String,Object>();
		if(filter!=null){
			for (String key : filter.keySet()) { //add "message." prefix to filter keys
			    filter2.put("msg."+key,filter.get(key));
			}
		}
	
		return mcm.countQuery(filter2);
	}
	
	public List<Message> findMessages(String recipient){
		Map<String,Object> filter = new HashMap<String,Object>();
		filter.put("recipient", recipient);
		return findBy(filter);
	}

	
}
