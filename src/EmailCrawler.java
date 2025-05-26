import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.FileInputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

public class EmailCrawler {

    private static final String START_URL = "https://www.touro.edu";
    private static final int MAX_EMAILS = 10000;
    private static final int BATCH_SIZE = 500;
    private static final int MAX_THREADS = 10;

    private static final Queue<String> urlQueue = new ConcurrentLinkedQueue<>();
    private static final Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
    private static final Set<String> foundEmails = ConcurrentHashMap.newKeySet();
    private static final List<EmailRecord> emailBatch = Collections.synchronizedList(new ArrayList<>());

    private static String dbUrl;
    private static String dbUser;
    private static String dbPass;

    public static void main(String[] args) throws Exception {
        loadDbConfig();
        long start = System.currentTimeMillis();

        urlQueue.add(START_URL);
        ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);

        while (true) {
            if (foundEmails.size() >= MAX_EMAILS) break;

            String url = urlQueue.poll();

            if (url == null) {
                if (((ThreadPoolExecutor) executor).getActiveCount() == 0 && urlQueue.isEmpty()) break;
                Thread.sleep(50);
                continue;
            }

            if (visitedUrls.contains(url)) continue;
            visitedUrls.add(url);

            executor.submit(() -> {
                try {
                    processUrl(url);
                } catch (Exception e) {
                    System.err.println("❌ Error at " + url + ": " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(15, TimeUnit.MINUTES);

        if (!emailBatch.isEmpty()) {
            saveToDb(emailBatch);
        }

        long end = System.currentTimeMillis();
        System.out.println("\n DONE!");
        System.out.println(" Emails scraped: " + foundEmails.size());
        System.out.println(" Pages visited: " + visitedUrls.size());
        System.out.println(" Time taken: " + (end - start) / 1000.0 + " seconds");
    }

    private static void processUrl(String url) throws Exception {
        System.out.println("🔍 Visiting: " + url);

        Document doc = Jsoup.connect(url).timeout(10000).get();

        // Queue new links
        for (Element link : doc.select("a[href]")) {
            String abs = link.absUrl("href");
            if (abs.isEmpty() || !abs.startsWith("http") || abs.contains("#") || visitedUrls.contains(abs)) continue;
            urlQueue.add(abs);
            System.out.println("➡️ Queued: " + abs + " | Queue size: " + urlQueue.size());
        }

        // Extract emails
        Pattern pattern = Pattern.compile("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-z]{2,}");
        Matcher matcher = pattern.matcher(doc.text());

        while (matcher.find() && foundEmails.size() < MAX_EMAILS) {
            String email = matcher.group();

            if (foundEmails.add(email)) {
                EmailRecord record = new EmailRecord(email, url);
                emailBatch.add(record);
                System.out.println(" [" + foundEmails.size() + "/" + MAX_EMAILS + "] " + email);

                if (emailBatch.size() >= BATCH_SIZE) {
                    List<EmailRecord> toSave;
                    synchronized (emailBatch) {
                        toSave = new ArrayList<>(emailBatch);
                        emailBatch.clear();
                    }
                    saveToDb(toSave);
                }
            }
        }
    }

    private static void saveToDb(List<EmailRecord> batch) {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
            String sql = "IF NOT EXISTS (SELECT 1 FROM Emails WHERE EmailAddress = ?) " +
                    "INSERT INTO Emails (EmailAddress, Source, TimeStamp) VALUES (?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (EmailRecord rec : batch) {
                    ps.setString(1, rec.getEmail());      // for IF NOT EXISTS
                    ps.setString(2, rec.getEmail());      // insert
                    ps.setString(3, rec.getSource());
                    ps.setTimestamp(4, rec.getTimestamp());
                    ps.addBatch();
                }
                ps.executeBatch();
                System.out.println("Saved batch of " + batch.size() + " emails");
            }
        } catch (SQLException e) {
            System.err.println("DB error: " + e.getMessage());
        }
    }

    private static void loadDbConfig() {
        try {
            Properties props = new Properties();
            props.load(new FileInputStream("res.properties"));
            dbUrl = props.getProperty("res.url");
            dbUser = props.getProperty("res.username");
            dbPass = props.getProperty("res.password");
        } catch (Exception e) {
            System.err.println("Could not load res.properties");
            System.exit(1);
        }
    }
}
