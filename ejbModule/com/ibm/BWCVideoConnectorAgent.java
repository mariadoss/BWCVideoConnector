package com.ibm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.net.ssl.SSLContext;
import javax.sql.DataSource;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
/**
 * Message-Driven Bean implementation class for: BWCVideoConnectorAgent
 */
@MessageDriven(
		activationConfig = { @ActivationConfigProperty(
				propertyName = "destinationType", propertyValue = "javax.jms.Queue")
		})
@TransactionManagement(TransactionManagementType.BEAN)
public class BWCVideoConnectorAgent implements MessageListener {
	String bwcMesage = null;
	CloseableHttpClient client = null;
	static String rootURL = "https://iva-ibm-mils/milsng/user/SVSProxy";
	private final String redactorURI = ""; 				//need a value
	private final String requestorId = ""; 				//need a value
	private final String sharedFileSystem =""; 			//need a value
	private final String loginURL = "/session";
	private final String JobURL = "/vfiJob";
	private final String BWCDBName = "jdbc/BWCDB";		//need a value
	
	String IVALoginString = "{ "
			+ "\"_type\" : \"session\", "
			+ "\"_markup\" : \"login\", "
			+ "\"Items\": "
			+ "[{ \"userId\" = \"pvadmin\", "       	//need a value
			+ "\"password\" = \"xgal9bI4tWkbBcH3\", "  	//need a value
			+ "\"locale\" = \"en\", "
			+ "\"mime\" = \"json\", "
			+ "\"timezone\" = \"America/New_York\", "
			+ "\"numberFormat\" = \"######.##\", "
			+ "\"timestampFormat\" = \"yyyy-MM-dd'T'HH.mm.ss.SSS\", "
			+ "\"dateFormat\" = \"yyyy-MM-dd\" "
			+ "}] "
			+ "}";

	private boolean useHTTPS = false;
	private boolean bConnected = false;
	private String ivaSessionToken = "";
	private String jSessionToken = "";
	private Connection connection;
	private DataSource dataSource;
	// used for the transaction rollback
	@Resource
	private MessageDrivenContext context;

	 
	  public void setDataSource()  {
		InitialContext ctx;
		try {
			ctx = new InitialContext();
			this.dataSource = (DataSource) ctx.lookup("java:comp/env/" +  BWCDBName);	 
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		  
	  }

	  @PostConstruct
	  public void initialize() {
		client = createHttpClient();
		try {
		   setDataSource();	
	       connection = dataSource.getConnection();	  
	       System.out.println("Successfully initialized the datasource of BWC meta-data MDB");
	    } catch (SQLException sqle) {
	         sqle.printStackTrace();
	    }
	    try{
	    	connectIVA();
	    }catch(Exception exp){
	    	exp.printStackTrace();
	    }
	  }

	  @PreDestroy
	  public void cleanup() {
	    try {
	      connection.close();
	      connection = null;
	    } catch (SQLException sqle) {
	        sqle.printStackTrace();
	    }
	    try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	  }
	
	/**
     * Default constructor. 
     */
    public BWCVideoConnectorAgent() {
        // TODO Auto-generated constructor stub
    }
	
	/**
     * @see MessageListener#onMessage(Message)
     */
    public void onMessage(Message message) {
        // TODO Auto-generated method stub
    	bwcMesage=null;
    	try {
    		if (message instanceof javax.jms.TextMessage) {
    			bwcMesage = ((javax.jms.TextMessage)message).getText();
    			processMetaData(bwcMesage);
    		}
    	}catch(Exception exp){
    		exp.printStackTrace();
    	}    		
    }
    
    public void processMetaData(String bwcMesage) {
    	String fileName = null;
    	JSONParser parser = new JSONParser();
		try {
			String processIVAURI = "";
			Object jsonObj = parser.parse(bwcMesage);
			JSONArray array = (JSONArray) jsonObj;
			//for (int i = 0; i < array.size(); i++) {
			{
				JSONObject jsonMetaData = (JSONObject) array.get(0);
				fileName = (String) jsonMetaData.get("name");
				processAuditRequest(fileName, "11"); //in-progress
				long startTime = (Long) jsonMetaData.get("startTime");
				Date startTimeInDate = new Date(startTime);
				JSONObject owner = (JSONObject) jsonMetaData.get("owner");
				String ownerName = (String) owner.get("name");
				String deviceName = (String) jsonMetaData.get("deviceName");
				Long incidentCount = (Long) jsonMetaData.get("incidentCount");
				
				System.out.println("fileName: " + fileName + " StartTime: " + startTimeInDate.toString()
						+ " ownerName: " + ownerName + " deviceName: " + deviceName + " incidentcount: " + incidentCount);
				
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH.mm.ss.SSS");
				processIVAURI = sharedFileSystem + fileName;
				String ownerNameToChannel = processIVAChannelRequest(ownerName);
				createJob(ownerNameToChannel, processIVAURI, dateFormat.format(startTimeInDate));
				processAuditRequest(fileName, "13"); //sent-to-IVA without exception
			}
			//}		
		} catch (Exception e) {
			e.printStackTrace();
			try{
				if (fileName != null){
					processAuditRequest(fileName, "12"); //sending-to-IVA without exception
				}	
			}catch (Exception exp){
				System.out.println("Exception on AuditRequest");
				e.printStackTrace();
			}
		}
	}
    
   
    private CloseableHttpClient createHttpClient() {

		CloseableHttpClient httpclient = null;
		if ( this.useHTTPS == true ) {
		@SuppressWarnings("deprecation")
		SSLContext sslContext = SSLContexts.createDefault();
		SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
				sslContext, new String[] { "TLSv1.2" }, null,
				new NoopHostnameVerifier());

		httpclient = HttpClients.custom()
					.setSSLSocketFactory(sslsf).build();
		
		} else {
			httpclient = HttpClients.createDefault();
		}
		return httpclient;
	}
    
    public boolean connectIVA() {
		if (bConnected && !ivaSessionToken.isEmpty())
			return bConnected;

		String urlString = rootURL + loginURL;
		System.out.println("Login to IVA server");

		HttpPost post = new HttpPost(urlString);
		CloseableHttpResponse response = null;

		try {
			StringEntity input = new StringEntity(IVALoginString);
			input.setContentType("application/json");
			post.setEntity(input);
			post.addHeader("Accept", "text/html, application/json");
			System.out.println("Connect POST content: ");
			response = client.execute(post);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return bConnected;
		} catch (IOException e1) {
			e1.printStackTrace();
			return bConnected;
		}

		int ResponseCode = response.getStatusLine().getStatusCode();
		if (ResponseCode == 200) {

			Header[] headers = response.getAllHeaders();
			for (Header header : headers) {
				System.out.println("Key: " + header.getName() + ", Value: "
						+ header.getValue());
				if (header.getName().equalsIgnoreCase("Set-Cookie")) {
					if (header.getValue().contains("IVASESSIONID")) {
						ivaSessionToken = parseSessionToken2(header.getValue());
					}
					if (header.getValue().contains("JSESSIONID")) {
						ivaSessionToken = parseSessionToken3(header.getValue());
					}
				}

			}
			if (ivaSessionToken != null && !ivaSessionToken.isEmpty())
				bConnected = true;
		} else {
			System.err.println("Connection Failed. " + ResponseCode
					+ "\n\tJSON sent: " + IVALoginString);
		}
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bConnected;
	}
    
    private String parseSessionToken2(String cookieValue) {

		this.ivaSessionToken = cookieValue.substring(
				cookieValue.indexOf('=') + 1, cookieValue.indexOf(';'));
		bConnected = true;
		System.out.println("IVA SESSION = " + this.ivaSessionToken);

		return this.ivaSessionToken;
	}
    
    private String parseSessionToken3(String cookieValue) {

		this.jSessionToken = cookieValue.substring(
				cookieValue.indexOf('=') + 1, cookieValue.indexOf(';'));
		bConnected = true;
		System.out.println("JSESSIONID = " + this.jSessionToken);

		return this.jSessionToken;
	}
    
    private boolean createJob( String groupingId, String processURI, String videoStartTS) {

		boolean retVal = false;

		CloseableHttpClient client = createHttpClient();

		String urlString = rootURL + JobURL + "/:createJob";
		System.out.println("VFI URL = " + urlString);
		System.out.println("File Name/Process URI = " + processURI);

		HttpPost post = new HttpPost(urlString);

		post.addHeader("X-IVA-Request", this.ivaSessionToken);
		post.addHeader("Accept", "text/html, application/json");
		post.addHeader("Content-Type", "application/json");
		post.addHeader("Cookie", "JSESSIONID=" + this.jSessionToken
				+ "; VASESSIONID=" + this.ivaSessionToken);
		
		StringBuilder sb = new StringBuilder();
		sb.append("{ \"_type\":\"vfiJob\",\"_markup\":\"createJob\", \"items\": [ {");
		sb.append(" \"groupingId\":\"$groupingId\", ");
		sb.append(" \"processUri\":\"$processUri\", ");
		sb.append(" \"redactUri\":\"$redactUri\", ");
		sb.append(" \"requestorId\":\"$requestorId\", ");
		sb.append(" \"redactStreaming\":\"$redactStreaming\", ");
		sb.append(" \"processRemote\":\"$processRemote\", ");
		sb.append(" \"enableFR\":\"$enableFR\", ");
		sb.append(" \"videoStartTS\":\"$videoStartTS\" ");
		sb.append("} ] }");
		String createJobJSON = sb.toString();
		
		String finalCreateJobJSON = createJobJSON.replace("$groupingId",
				groupingId);
		finalCreateJobJSON = finalCreateJobJSON.replace("$processUri",
				processURI);
		finalCreateJobJSON = finalCreateJobJSON.replace("$redactUri",
				redactorURI);
		finalCreateJobJSON = finalCreateJobJSON.replace("$requestorId",
				requestorId);
		finalCreateJobJSON = finalCreateJobJSON.replace("$redactStreaming",
				"false");
		finalCreateJobJSON = finalCreateJobJSON.replace("$processRemote",
				"DEFAULT");
		finalCreateJobJSON = finalCreateJobJSON.replace("$enableFR", "true");
		finalCreateJobJSON = finalCreateJobJSON.replace("$videoStartTS",
				videoStartTS);

		CloseableHttpResponse response = null;

		try {
			StringEntity entity = new StringEntity(finalCreateJobJSON);
			post.setEntity(entity);

			response = client.execute(post);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		Header[] headers = post.getAllHeaders();
		System.out.println("POST Header");
		for (Header header : headers) {
			System.out.println("Key: " + header.getName() + ", Value: "
					+ header.getValue());
		}

		int responseCode = response.getStatusLine().getStatusCode();

		if (responseCode == 200) {
			System.out.println("VFI Job Created Successfully");
			System.out.println("Response is: " + response.toString());
			retVal = true;
		} else {
			System.err.println("VFI Job Create Failed: " + responseCode);
			headers = response.getAllHeaders();
			System.err.println("Response: " + response.toString());
			System.err.println("Response Header");
			for (Header header : headers) {
				System.err.println("Key: " + header.getName() + ", Value: "
						+ header.getValue());
			}
		}
		return retVal;
	}
    
 // This method would use JPA in the real world to persist the data   
    private String processIVAChannelRequest(String user) throws SQLException {
      System.out.println("Making an IVA channel request for user " + user);
      String channel = null;
      try{
    	  Statement statement = connection.createStatement();
    	  String SelectChannelSql = "SELECT CHANNEL FROM " + 
    			  "BWCUserToChannel WHERE USER = '" + user + "'";
    	  System.out.println(SelectChannelSql);
    	  ResultSet result= statement.executeQuery(SelectChannelSql);
    	  if(result.next()) {
    		  channel = result.getString    ("CHANNEL");
    	  }
    	  statement.close();
      }catch(Exception exp){
    	  exp.printStackTrace();
      }
      System.out.println("The IVA channel extracted is " + channel);
      return channel;
    }
    
 // This method would use JPA in the real world to persist the data   
    private void processAuditRequest(String filename, String Status) throws SQLException {
      System.out.println("Making an Audit request with status " + Status);
      try{
    	  Statement statement = connection.createStatement();
    	  String AuditSQl = "INSERT INTO " + "BWCAUDIT (FILENAME, STATUS) VALUES"
    	  		+ "(" + "'" + filename + "'," +  Status + ")";
    	  System.out.println(AuditSQl);
    	  statement.execute(AuditSQl);
    	  statement.close();
      }catch(Exception exp){
    	  exp.printStackTrace();
      }
    }
}
