
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class LuceneIndexer
{

	public static String property = System.getProperty("user.home");
    public static String INDEX_DIR= property + "\\Indexes";
	private static final String JDBC_DRIVER = "org.apache.cassandra.cql.jdbc.CassandraDriver";
	private static final String CONNECTION_URL = "jdbc:cassandra://10.20.3.163:9160/caerus_jdbcdriver?version=3.0.0";
	private static final String USER_NAME = "";
	private static final String PASSWORD = "";
	
	// create indexing query for jobs and internships 
	private static final String QUERY_FOR_JOB = "select job_id_and_firm_id,primary_skills,secondary_skills,location,functional_area," +
			"industry,posted_on,job_title from job_dtls where status=\'Published\'";
	
	private static final String QUERY_FOR_INTERNSHIP = "select internship_id_and_firm_id,primary_skills,secondary_skills,location," +
			"internship_title,posted_on from internship_dtls where status = \'Published\'";
	
	
	
	public static void main(String[] args) 
	{
	//Creating Index Directory
           System.out.println("Hello");		
	   File indexDir = new File(INDEX_DIR);
		
		if (!indexDir.exists()) 
		{
			System.out.println("Creating Directory...");
			indexDir.mkdir(); 
			
		}		
		
		//Existing indexes are deleted and new indexes created
		String[] myFiles= indexDir.list();
				
					if(myFiles.length > 0)
					{
						System.out.println("Deleting files...");
					  for (int i = 0; i < myFiles.length; i++) 
					  {
			                File myFile = new File(indexDir, myFiles[i]);
			                System.out.println(myFile);
			                myFile.delete();
			          }
					}
				
					LuceneIndexer indexer = new LuceneIndexer();
		
        	try
        	{  
        			   
        				Class.forName(JDBC_DRIVER).newInstance();  
        			   Connection conn = DriverManager.getConnection(CONNECTION_URL, USER_NAME, PASSWORD);  
        			   
        			   //Using Standard Analyser for indexing
        			   
        			   StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);  
        			   IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_35, analyzer);
        			   IndexWriter indexWriter = new IndexWriter(FSDirectory.open(indexDir), indexWriterConfig);
        			   
        			   System.out.println("Indexing to directory '" + indexDir + "'...");  
        			  
        			   int indexForJobsCount = indexer.indexForJobs(indexWriter,conn);
        			   
        			   System.out.println(indexForJobsCount + " job records have been indexed successfully");
        			   
        			   int indexForInternshipCount = indexer.indexForInternships(indexWriter,conn);
        			   
        			   System.out.println(indexForInternshipCount + " internship records have been indexed successfully");      			   
        			  
        			   indexWriter.close();  
        			 
        			   
        	} 
        	catch (Exception e)
        	{  
        		e.printStackTrace();  
        	} 
   		
	}
	
	
	@SuppressWarnings("deprecation")
	private int indexForJobs(IndexWriter indexWriter, Connection conn) throws SQLException, IOException {
		
		String sql="";
	    ResultSet rs=null;   
	    Statement stmt = conn.createStatement();  
	    int i=0;
	    

		
	      sql=QUERY_FOR_JOB;
	      rs= stmt.executeQuery(sql);
	      
	      while (rs.next()) 
		  {  
		     Document d = new Document();  
		     d.add(new TextField("job_id_and_firm_id", rs.getString("job_id_and_firm_id"), Field.Store.YES)) ;
		     
		     
		     String localPrimarySkills="";
			    
		     if(null!=rs.getString("primary_skills"))
		     {
		    	 localPrimarySkills=rs.getString("primary_skills");
		    	 if(localPrimarySkills.contains("["))
		    	 {
		    		 if(localPrimarySkills.contains("]"))
			    	 {
			    		 localPrimarySkills=localPrimarySkills.replace("]","");
			    	 }
		    		 
		    		 localPrimarySkills=localPrimarySkills.replace("[","");
		    	 }
		    	 
		    	 Field primarySkillField=new Field("primary_skills_for_jobs",localPrimarySkills, Field.Store.YES,Field.Index.ANALYZED);
		    	 primarySkillField.setBoost(2.0f);
		    	 d.add(primarySkillField) ;  
			  
			 System.out.println("Final Primary Skills: "+localPrimarySkills);
		     }
		     
		     else
		    	  d.add(new TextField("primary_skills_for_jobs",localPrimarySkills, Field.Store.YES)) ;  
		     
		     String localSecondarySkills="";
			    
		 if(null!=rs.getString("secondary_skills"))
		     {
		    	 localSecondarySkills=rs.getString("secondary_skills");
		    	 if(localSecondarySkills.contains("["))
		    	 {
		    		 if(localSecondarySkills.contains("]"))
			    	 {
		    			 localSecondarySkills=localSecondarySkills.replace("]","");
			    	 }
		    		 
		    		 localSecondarySkills=localSecondarySkills.replace("[","");
		    	 }
		    	 
			    // d.add(new TextField("secondary_skills",localSecondarySkills, Field.Store.YES)) ; 
		    	 Field secondarySkillField=new Field("secondary_skills_for_jobs",localSecondarySkills, Field.Store.YES,Field.Index.ANALYZED);
		    	 //secondarySkillField.setBoost(2.0f);
		    	 d.add(secondarySkillField) ; 
			  
			     System.out.println("Final Secondary Skills: "+localSecondarySkills);
		     }
		     
		     else
		    	  d.add(new TextField("secondary_skills_for_jobs",localSecondarySkills, Field.Store.YES)) ;  
		     
		     
		     
		     if(null!=rs.getString("location"))
			     d.add(new TextField("location_for_jobs", rs.getString("location"), Field.Store.YES));  
			     else
				 d.add(new TextField("location_for_jobs", " ", Field.Store.YES)) ;
		     
		     
		     if(null!=rs.getString("functional_area"))
			     d.add(new TextField("functional_area", rs.getString("functional_area"), Field.Store.YES));  
			     else
				 d.add(new TextField("functional_area", " ", Field.Store.YES)) ;
		     
		     
		     if(null!=rs.getString("industry"))
			     d.add(new TextField("industry", rs.getString("industry"), Field.Store.YES));  
			     else
				 d.add(new TextField("industry", " ", Field.Store.YES)) ;
		       
		     
		     if(null!=rs.getString("job_title"))
			     d.add(new TextField("job_title", rs.getString("job_title"), Field.Store.YES));  
			     else
				 d.add(new TextField("job_title", " ", Field.Store.YES)) ;

		        if( rs.getDate("posted_on")!=null)
		            
		        { 
		        	 
			    	Field dateField = new LongField("job_posted_on",rs.getDate("posted_on").getTime(), Field.Store.YES);
//			        dateField.setBoost(2.0f);   
			        d.add(dateField);
			    	//Date ts=new Date(Long.parseLong(d.get("job_posted_on")));
	
		          // d.add(new LongField("job_posted_on",rs.getDate("posted_on").getTime(), Field.Store.YES));
		           //Date ts=new Date(Long.parseLong(d.get("job_posted_on")));
		          // System.out.println("ts: "+ts);
		        }
		        else
		            d.add(new LongField("job_posted_on",0, Field.Store.YES));
		     
		     
		        indexWriter.addDocument(d);  
		     i++;
		    
		 }
		return i;
	  
	     
	}
	
	
	@SuppressWarnings("deprecation")
	private int indexForInternships(IndexWriter indexWriter, Connection conn) throws SQLException, IOException {  
	     String sql="";
	     ResultSet rs=null;   
	     Statement stmt = conn.createStatement();  
		    
		  int i=0;
		
			  sql=QUERY_FOR_INTERNSHIP;
		      rs= stmt.executeQuery(sql);
		      
		      while (rs.next()) 
			  {  
			     Document d = new Document();  
			     d.add(new TextField("internship_id_and_firm_id", rs.getString("internship_id_and_firm_id"), Field.Store.YES)) ;
			     
			     
			     String localPrimarySkills="";
				    
			     if(null!=rs.getString("primary_skills"))
			     {
			    	 localPrimarySkills=rs.getString("primary_skills");
			    	 if(localPrimarySkills.contains("["))
			    	 {
			    		 if(localPrimarySkills.contains("]"))
				    	 {
				    		 localPrimarySkills=localPrimarySkills.replace("]","");
				    	 }
			    		 
			    		 localPrimarySkills=localPrimarySkills.replace("[","");
			    	 }
			    	 
			    	 Field primarySkillField=new Field("primary_skills_for_internships",localPrimarySkills, Field.Store.YES,Field.Index.ANALYZED);
			    	 primarySkillField.setBoost(2.0f);
			    	 d.add(primarySkillField) ;  
				  
				 System.out.println("Final Primary Skills: "+localPrimarySkills);
			     }
			     
			     else
			    	  d.add(new TextField("primary_skills_for_internships",localPrimarySkills, Field.Store.YES)) ;  
			     
			     String localSecondarySkills="";
				    
			 if(null!=rs.getString("secondary_skills"))
			     {
			    	 localSecondarySkills=rs.getString("secondary_skills");
			    	 if(localSecondarySkills.contains("["))
			    	 {
			    		 if(localSecondarySkills.contains("]"))
				    	 {
			    			 localSecondarySkills=localSecondarySkills.replace("]","");
				    	 }
			    		 
			    		 localSecondarySkills=localSecondarySkills.replace("[","");
			    	 }
			    	 
				     //d.add(new TextField("secondary_skills",localSecondarySkills, Field.Store.YES)) ;
			    	 
			    	 Field secondarySkillField=new Field("secondary_skills_for_internships",localSecondarySkills, Field.Store.YES,Field.Index.ANALYZED);
			    	 //secondarySkillField.setBoost(2.0f);
			    	 d.add(secondarySkillField) ; 
				  
				     System.out.println("Final Secondary Skills: "+localSecondarySkills);
			     }
			     
			     else
			    	  d.add(new TextField("secondary_skills_for_internships",localSecondarySkills, Field.Store.YES)) ;  
			     
			     
			     
			     if(null!=rs.getString("location"))
				     d.add(new TextField("location_for_internships", rs.getString("location"), Field.Store.YES));  
				     else
					 d.add(new TextField("location_for_internships", " ", Field.Store.YES)) ;
			     
			     if(null!=rs.getString("internship_title"))
				     d.add(new TextField("internship_title", rs.getString("internship_title"), Field.Store.YES));  
				     else
					 d.add(new TextField("internship_title", " ", Field.Store.YES)) ;

			        if( rs.getDate("posted_on")!=null)
			            
			        { 
			        	
				        Field dateField= new Field("internship_posted_on",rs.getString("posted_on"), Field.Store.YES,Field.Index.ANALYZED);
				        
				        dateField.setBoost(2.0f);
			         
				     //   Date ts=new Date(Long.parseLong(d.get("internship_posted_on")));
			          // System.out.println("ts: "+ts);
				        
				        d.add(dateField);
			        }
			        else
			            d.add(new LongField("internship_posted_on",0, Field.Store.YES));
			     
			     
			     indexWriter.addDocument(d);  
			     i++;
			  }
		  
		  return i;
	}



}
