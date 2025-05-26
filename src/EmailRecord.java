import java.sql.Timestamp;

public class EmailRecord {
    private final String email;
    private final String source;
    private final Timestamp timestamp;

    public EmailRecord(String email, String source) {
        this.email = email;
        this.source = source;
        this.timestamp = new Timestamp(System.currentTimeMillis());
    }

    public String getEmail() { return email; }
    public String getSource() { return source; }
    public Timestamp getTimestamp() { return timestamp; }
}
