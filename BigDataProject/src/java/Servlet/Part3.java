package Servlet;

import bean.Num;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import org.neo4j.driver.v1.*;

public class Part3 extends HttpServlet {

   
   Num num = new Num();
   String name[];
   Record record = null;

   public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
       String number = request.getParameter("number");
       String id = request.getParameter("id");
       String title = request.getParameter("title");
       String hotness = request.getParameter("hotness");
       String duration = request.getParameter("duration");
       String name = request.getParameter("name");
       String location = request.getParameter("location");
       String count = request.getParameter("count");
       String played = request.getParameter("played");
       
       num.setNumber((number));
       num.setId(id);
       num.setTitle(title);
       num.setHotness(hotness);
       num.setDuration(duration);
       num.setLocation(location);
       num.setCount(count);
       num.setPlayed(played);
       
       if(number != null){
       Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "123456" ) );
    		Session session = driver.session();
    		StatementResult result = session.run("MATCH (song {songid: '"+ number +"'}) RETURN "
                        + "song.Title as SongTitle, song.Hotness as SongHotness, song.Artist_name as ArtistName,"
                        + "song.Num_Played as SongViews, song.Duration as Duration, song.User_count as Viewers,"
                        + "song.Artist_location as Location;");
                System.out.println(result+"===========================================");
                if ( result.hasNext() )
    		{
    		   record = result.next();
                   sendPage1(response);
    		}else{
                sendPage2(request, response);
                }
       }else if(id!=null){
           Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "123456" ) );
    		Session session = driver.session();
    		StatementResult result = session.run( "CREATE(song:Song{songid:'" + id + "',"
                        + "Title:'" + title + "'," + "Hotness:'" + hotness + "'," + "Duration:'" + duration + "',"
                        + "Artist_name:'" + name + "'," + "Artist_location:'" + location + "'," + "User_count:'" + count + "',"
                        + "Num_Played:'" + played + "'})");
     
                sendPage3(request, response);
           
       }
                
   }
   
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
       
       String page = request.getParameter("page");
       
       if (page == null) {
           sendPage1(response);
           return;
       }

   }
   
   void sendPage1(HttpServletResponse response) throws ServletException, IOException {
       response.setContentType("text/html");
       PrintWriter out = response.getWriter();
      
       out.println("<HTML>");
       out.println("<HEAD><TITLE>Big Data Project</TITLE></HEAD>");
       out.println("<style>table, td, th {border: 1px solid white;}table {border-collapse: collapse;width: 80%;}</style>");
       out.println("<BODY bgcolor=\"orangered\">");
       out.println("<CENTER>");
       out.println("<H2><font color=\"white\">Details For The Desired Song ID</font></H2>");
       out.println("<FORM action ='form' METHOD=POST>");
       out.println("<br><br><INPUT TYPE=HIDDEN NAME=page VALUE=1>");
       out.println("<Table border='1'>");
       out.println("<TR>");
       out.println("<TH><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>Song-ID</center></font></p></TH>");
       out.println("<TH><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>Title</center></font></p></TH>");
       out.println("<TH><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>Artist Name</center></font></p></TH>");
       out.println("<TH><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>Song Hotness</center></font></p></TH>");
       out.println("<TH><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>Duration</center></font></p></TH>");
       out.println("<TH><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>User Count</center></font></p></TH>");
       out.println("<TH><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>Times Played</center></font></p></TH>");
       out.println("<TH><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>Artist Location</center></font></p></TH>");

       out.println("</TR>");
       out.println("<TR>");
       out.println("<TD><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>"+num.getNumber()+"</center></font></p></TD>");
       out.println("<TD><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>"+record.get("SongTitle")+"</center></font></p></TD>");
       out.println("<TD><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>"+record.get("ArtistName")+"</center></font></p></TD>");
       out.println("<TD><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>"+record.get("SongHotness")+"</center></font></p></TD>");
       out.println("<TD><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>"+record.get("Duration")+"</center></font></p></TD>");
       out.println("<TD><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>"+record.get("Viewers")+"</center></font></p></TD>");
       out.println("<TD><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>"+record.get("SongViews")+"</center></font></p></TD>");
       out.println("<TD><p style=\"text-shadow:2px 2px 8px peachpuff;\"><font color=\"white\"><center>"+record.get("Location")+"</center></font></p></TD>");
       out.println("</TR>");
       out.println("</Table>");
       out.println("</FORM>");
       out.println("<a href='index.html'><font color=\"white\">Go To Home</font></a>");
       out.println("</CENTER>");
       out.println("</BODY>");
       out.println("</HTML>");
   }
   
   void sendPage2(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
       response.setContentType("text/html");
       PrintWriter out = response.getWriter();
      
       out.println("<HTML>");
       out.println("<HEAD><TITLE>Big Data Project</TITLE></HEAD>");
       out.println("<style>table, td, th {border: 1px solid white;}table {border-collapse: collapse;width: 80%;}</style>");
       out.println("<BODY bgcolor=\"orangered\">");
       out.println("<CENTER>");
       out.println("<H2><font color=\"white\">Desired Song ID Does Not Exist</font></H2>");
       out.println("<a href='index.html'><font color=\"white\">Go To Home</font></a>");
       out.println("</CENTER>");
       out.println("</BODY>");
       out.println("</HTML>");
   }
   void sendPage3(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
       response.setContentType("text/html");
       PrintWriter out = response.getWriter();
      
       out.println("<HTML>");
       out.println("<HEAD><TITLE>Big Data Project</TITLE></HEAD>");
       out.println("<style>table, td, th {border: 1px solid white;}table {border-collapse: collapse;width: 80%;}</style>");
       out.println("<BODY bgcolor=\"orangered\">");
       out.println("<CENTER>");
       out.println("<H2><font color=\"white\">Desired Song Has Been Added</font></H2>");
       out.println("<a href='index.html'><font color=\"white\">Go To Home</font></a>");
       out.println("</CENTER>");
       out.println("</BODY>");
       out.println("</HTML>");
   }
}