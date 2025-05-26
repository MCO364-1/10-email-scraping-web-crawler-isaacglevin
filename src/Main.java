import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {

    }

    //Pseudocode: The single-threaded version of this crawler starts at the root URL https://www.touro.edu and uses a queue to perform a breadth-first search through web pages. It maintains a set of visited URLs to ensure pages aren’t crawled more than once, and a set of found emails to avoid duplicates. For each page, it extracts all links and adds any new ones to the queue, then scans the page’s text using a regular expression to find email addresses. New emails are stored in memory as EmailRecord objects (which include the email, source URL, and timestamp). These are batched in groups of 500 and inserted into the SQL Server Emails table. The crawl continues until either the queue is empty or 10,000 unique emails have been collected, with one final insert for any remaining emails after the loop ends.
}