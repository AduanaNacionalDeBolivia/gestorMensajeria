package bo.aduana.gestorMensajeria.model;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.DBObject;

public class Message {
	private static final long serialVersionUID = -2145878502099973973L;
	private static final Logger log = Logger.getLogger(Message.class);
	private String _id;
	private String type;
	private String sender;
	private Long time;
	private Date received;
	private String body;
	private HashMap<String,Object> processedObject;
	private String status;
	private String recipient;
	
	
	public Message(){
		
	}
	
	public Message(String id, DBObject message){
		_id = id;
		this.type = message.get("type").toString();
		this.sender = message.get("sender").toString();
		this.body = message.get("body").toString();
		this.time = Long.parseLong(message.get("time").toString());
		this.recipient = message.get("recipient").toString();
		if(message.get("status")!=null)
			this.status = message.get("status").toString();
	}
	
	public Message(String type, String sender, String recipient, Long time, String body, String status){
		this.type = type;
		this.sender = sender;
		this.time = time;
		this.body = body;
		this.received = new Date();
		this.status = status;
		this.recipient = recipient;
	}
	
	
	public String toJsonStr(){
		JSONObject json = new JSONObject();
		try {

			json.put("sender", sender);
			json.put("type", type);
			json.put("received", sdf.format(received));
			json.put("status", status);
			json.put("recipient", recipient);
			if(time!=null){
				json.put("time", time);
			}
			
			if(body!=null && !body.isEmpty()){
				try {
					JSONObject jsonProcessed = new JSONObject(body);
					json.put("body", jsonProcessed);
				} catch (JSONException e) {
					log.error("JSON's Body is invalid, GestorMensajeria will persist it as a \"string\"",e);
					json.put("body", body);
				}
			}else{
			    json.put("body",JSONObject.NULL);
			}
			
		} catch (JSONException e) {
			Exception ex = new Exception("Error in BEAN->JSON construction",e);
			log.error(ex);
		}
		
		return json.toString();
	}

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @return the _id
	 */
	public String get_id() {
		return _id;
	}

	/**
	 * @param _id the _id to set
	 */
	public void set_id(String _id) {
		this._id = _id;
	}
	
	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the sender
	 */
	public String getSender() {
		return sender;
	}

	/**
	 * @param sender the sender to set
	 */
	public void setSender(String sender) {
		this.sender = sender;
	}

	/**
	 * @return the time
	 */
	public Long getTime() {
		return time;
	}

	/**
	 * @param time the time to set
	 */
	public void setTime(Long time) {
		this.time = time;
	}

	/**
	 * @return the body
	 */
	public String getBody() {
		return body;
	}

	/**
	 * @param body the body to set
	 */
	public void setBody(String body) {
		this.body = body;
	}

	/**
	 * @return the received
	 */
	public Date getReceived() {
		return received;
	}
	
	
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public HashMap<String,Object> getProcessedObject() {
		return processedObject;
	}

	public void setProcessedObject(HashMap<String,Object> processed) {
		this.processedObject = processed;
	}

	public String getRecipient() {
		return recipient;
	}

	public void setRecipient(String recipient) {
		this.recipient = recipient;
	}

	public void setReceived(Date received) {
		this.received = received;
	}
	
	
	
}
