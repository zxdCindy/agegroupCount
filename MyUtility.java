package question1;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author cindyzhang
 * Utility functions
 */
public class MyUtility {
	
	public static  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
					.split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				//System.out.println(key + ": " + val);
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}
	
	public static  String nestElements(String user, List<String> postComments) 
			throws ParserConfigurationException, 
					SAXException, IOException, TransformerException{
		DocumentBuilder bldr = dbf.newDocumentBuilder();
		Document doc = bldr.newDocument();
		
		Element userEl = getXmlElementFromeString(user);
		Element toAddUserEl = doc.createElement("user");
		
		copyAttributesToElement(userEl.getAttributes(), toAddUserEl);
		
		for(String chilXml:postComments){
			String[] comments = chilXml.split("\t");
			
			//AddPost
			Element postEl = getXmlElementFromeString(comments[0]);
			Element toAddPostEl = doc.createElement("posts");
			copyAttributesToElement(postEl.getAttributes(), toAddPostEl);
			int n=0;
			for(String commenXml:comments){
				if(n!=0){
					Element commenEl = getXmlElementFromeString(commenXml);
					Element toAddCommenEl = doc.createElement("comment");
					copyAttributesToElement(commenEl.getAttributes(), toAddCommenEl);
					toAddPostEl.appendChild(toAddCommenEl);
				}
				n++;
			}
			
			toAddUserEl.appendChild(toAddPostEl);
		}
		
		doc.appendChild(toAddUserEl);
		return transformDocumentTOString(doc);
		
	}
	
	public static  Element getXmlElementFromeString(String xml) 
			throws SAXException, IOException, ParserConfigurationException{
		DocumentBuilder bldr = dbf.newDocumentBuilder();
		return bldr.parse(new InputSource(new StringReader(xml))).getDocumentElement();
	}
	
	public static  void copyAttributesToElement(NamedNodeMap attributes, Element element){
		for(int i=0;i< attributes.getLength();i++){
			Attr toCopy = (Attr)attributes.item(i);
			element.setAttribute(toCopy.getName(), toCopy.getValue());
		}
	}
	
	public static  String transformDocumentTOString(Document doc) 
			throws TransformerException{
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		StringWriter writer = new StringWriter();
		transformer.transform(new DOMSource(doc), new StreamResult(writer));
		return writer.getBuffer().toString().replaceAll("\n|\r", "");
	}

}
