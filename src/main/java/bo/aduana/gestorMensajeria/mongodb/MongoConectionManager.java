package bo.aduana.gestorMensajeria.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import bo.aduana.gestorMensajeria.model.Message;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

public class MongoConectionManager {

	private static Logger log = Logger.getLogger(MongoConectionManager.class);
	
	public enum PropertyKeys {
		HOST("host","localhost"),PORT("port","27017"),USER("user",null),PASS("pass",null);
		
		private String key;
		private String value;
		
		PropertyKeys(String key,String value){
			this.key = key;
			this.value=value;
		}
		
		public String getKey(){
			return key;
		}
		
		public String getDefault(){
			return value;
		}
	}
	
	private static Properties config;
	
	private DB db;
	
	private DBCollection collection;
	
	private static MongoConectionManager instance;
	
	private Mongo m;
	
	private String lastErrorMsg;
	
	private Boolean hasError = false;
	
	private boolean authenticate = false;

	private boolean authenticated = false;
	
	private String[] credentials = new String[2];
	
	private ObjectId id = null;
	
	public MongoConectionManager(){
		log.info("Creating MongoGateway Instance");
		String host = null;
		Integer port = null;
		String user = null;
		String pass = null;
		try {
			log.debug("Retrieving mongo_config.properties");
			InputStream stream = this.getClass().getClassLoader().getResourceAsStream("mongo_config.properties");
			config = new Properties();
			try {
				config.load(stream);
			} catch (IOException e) {
				try {
					throw new Exception(e);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			
			log.debug("Retrieving host value");
			host = config.getProperty(PropertyKeys.HOST.getKey(),PropertyKeys.HOST.getDefault());
			log.debug("Retrieving port value");
			port = Integer.valueOf(config.getProperty(PropertyKeys.PORT.getKey(),PropertyKeys.PORT.getDefault()));
			log.debug("Retrieving user value");
			user = config.getProperty(PropertyKeys.USER.getKey(),PropertyKeys.USER.getDefault());
			log.debug("Retrieving pass value");
			pass = config.getProperty(PropertyKeys.PASS.getKey(),PropertyKeys.PASS.getDefault());
			if((user!=null && pass!=null) && (!user.isEmpty() && !pass.isEmpty())){
				authenticate = true;
				credentials[0] = user;
				credentials[1] = pass;
			}

		} catch (MissingResourceException e) {
			log.warn("No MongoDB configuration file found, using default values",e);
			host = PropertyKeys.HOST.getDefault();
		}
		
		try {
			if(port!=null && port.intValue()!=0){
				m = new Mongo(host,port);
			}else m = new Mongo(host);
		} catch (Exception e) {
			System.out.println("Mongo Exception");
		}
		useDefaultDB();
		useCollection("mgs");
	}
	
	/**
	 * Retrieve MongoGateway instance
	 * @return Singleton instance of MongoGateway
	 */
	public static MongoConectionManager getInstance(){
		log.debug("Retrieving MongoGateway Instance");
		
		if(instance == null){
			instance = new MongoConectionManager();
		}
		
		return instance;
	}
	
	private DB getDB(){
		if(db==null)
			try {
				throw new Exception("Gateway is not connected", new NullPointerException("Null DB attribute"));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return db;
	}
	
	private DBCollection getCollection(){
		if(collection==null)
			try {
				throw new Exception("No collection selected", new NullPointerException("Null Collection attribute"));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return collection;
	}
	
	public String getLastErrorMsg(){
		return lastErrorMsg;
	}
	
	public boolean hasError(){
		return hasError;
	}
	
	private void processWriteResult(WriteResult wr){
		lastErrorMsg = wr.getError();
		hasError = !(lastErrorMsg == null);
	}
	
	private void reset(){
		lastErrorMsg = null;
		hasError = false;
	}
	
	private DBObject getDBO(Object obj) {
		return (DBObject) obj;
	}
	
	/*------GATEWAY ACTION METHODS------*/
	
	/**
	 * Connects to <b>&quot;pathfinder&quot;</b> shard database
	 * @see com.leafnoise.pathfinder.mongo.MongoGateway#connect(String)
	 * @return the current instance of the MongoGateway
	 */
	public MongoConectionManager useDefaultDB(){
		return useDB("messages");
	}
	
	/**
	 * Connects to a given mongo shard database
	 * @param dbStr The name of the mongo DB to retrieve, if none is found, mongo creates one with specified name.
	 * @return the current instance of the MongoGateway
	 */
	public MongoConectionManager useDB(String dbStr){
		if(dbStr == null || dbStr.trim().isEmpty()){
			useDefaultDB();
		}else{
			try {
				db = m.getDB(dbStr);
				if(authenticate && !authenticated){
					db.authenticate(credentials[0], credentials[1].toCharArray());
					authenticated = true;
				}
			} catch (Exception e) {
				log.error(e);
				try {
					throw new Exception("Unable to use \""+dbStr+"\" DB.",e);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		log.debug("Using DB: \""+db.getName()+"\"");
		return this;
	}
	
	/**
	 * Retrieves the specified collection.
	 * @param col the name of the collection to retrieve
	 * @return the current instance of the MongoGateway
	 */
	public MongoConectionManager useCollection(String col){
		if(col == null) return this;
		collection = getDB().getCollection(col);
		log.debug("Using collection: \""+collection.getName()+"\"");
		return this;
	}
	
	/**
	 * Persists a Map&lt;String,Object&gt; under a key of value &quot:message&quot;
	 * @param jsonMap the mapped valid JSON
	 * @return the current instance of the MongoGateway
	 */
	public MongoConectionManager persist(String message){
		return persist("msg",message);
	}
	
	/**
	 * Persists a Map&lt;String,Object&gt; under a given key
	 * @param jsonMap the mapped valid JSON
	 * @return the current instance of the MongoGateway
	 */
	public MongoConectionManager persist(String key, String message){
		reset();
		BasicDBObject dbo = new BasicDBObject();		
		Object msg;
		try {
			msg = JSON.parse(message);
			dbo.put(key,msg);
			WriteResult wr = getCollection().insert(dbo);	
			this.setLastId(new ObjectId(dbo.get("_id").toString()));
			processWriteResult(wr);
		} catch (JSONParseException e) {
			lastErrorMsg = e.getMessage();
			hasError = true;
		}
		return this;
	}
	
	/**
	 * Find all messages
	 * @return List&lt;PFMessage&gt;
	 */
	public List<Message> findAll(){
		return find(null);
	}
	
	/**
	 * Find messages according to filter object specifications<br/>
	 * If query map is null a generic find() will be executed.
	 * @param query the Map&lt;String,Object&gt; with the filters for the specific search
	 * @return List&lt;PFMessage&gt;
	 */
	public List<Message> find(Map<String,Object> query){
		List<Message> msgs = new ArrayList<Message>();
		DBCursor cursor = null;
		if(query==null){//find all
			 cursor = getCollection().find();
		}else{//find with filters
			cursor = getCollection().find(new BasicDBObject(query));
		}
		if(cursor!=null){
			try {
				while(cursor.hasNext()) {
					DBObject obj = cursor.next();
					DBObject evtIns = getDBO(obj.get("msg"));
					Message evt = new Message(obj.get("_id").toString(),evtIns);
					msgs.add(evt);
				}
			}catch(Exception e){
				log.error(e);
				try {
					throw new Exception(e);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}finally {
				cursor.close();
			}
		}
		return msgs;
	}
	
	public int countQuery(Map<String,Object> query){
		int rowsQ;
		if(query==null){//find all
			rowsQ = getCollection().find().count();
		}else{//find with filters
			rowsQ = getCollection().find(new BasicDBObject(query)).count();
		}
		
		return rowsQ;
	}
	
	public List<Message> findPaginate(Map<String,Object> query, int pageNumber, int nPerPage){
		List<Message> msgs = new ArrayList<Message>();
		DBCursor cursor = null;
		if(query==null){//find all
			cursor = getCollection().find().skip((pageNumber)*nPerPage).limit(nPerPage);
		}else{//find with filters
			cursor = getCollection().find(new BasicDBObject(query)).skip((pageNumber)*nPerPage).limit(nPerPage);
		}
		if(cursor!=null){
			try {
				while(cursor.hasNext()) {
					DBObject obj = cursor.next();
					DBObject evtIns = getDBO(obj.get("event"));
					Message evt = new Message(obj.get("_id").toString(),evtIns);
					msgs.add(evt);
				}
			}catch(Exception e){
				log.error(e);
				try {
					throw new Exception(e);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}finally {
				cursor.close();
			}
		}
		return msgs;
	}
	
	public DBCursor findSkipped(Map<String,Object> query, int nPerPage, int lastFound){
		DBCursor cursor = null;
		cursor = getCollection().find(new BasicDBObject(query)).skip(lastFound + 1).limit(nPerPage);
		return cursor;
	}

	public ObjectId getLastId() {
		return id;
	}

	public void setLastId(ObjectId id) {
		this.id = id;
	}
	
}