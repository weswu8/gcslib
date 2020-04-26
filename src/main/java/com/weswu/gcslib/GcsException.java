package main.java.com.weswu.gcslib;
import java.io.IOException;

public class GcsException extends IOException{
    private static final long serialVersionUID = 1L;

    public GcsException(String message) {
        super(message);
    }

    public GcsException(String message, Throwable cause) {
        super(message, cause);
    }

    public GcsException(Throwable t) {
        super(t);
    }
}
