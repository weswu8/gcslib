package main.java.com.weswu.gcslib;

public class Constants {
    public static final String	DEFAULT_CHARSET = "UTF-8";
    public static final String	DEFAULT_CONTENT_TYPE = "text/plain";
    public static final String	PATH_DELIMITER = "/";
    public static final int 	BLOB_BUFFERED_INS_DOWNLOAD_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304
    public static final int 	BLOB_BUFFERED_OUTS_BUFFER_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304
    public static final int BLOB_BUFFERED_OUTS_CHUNK_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304
    public static final int 	BLOB_BUFFERED_OUTS_APPENDBLOB_CHUNK_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304
    public static final int 	BLOB_BUFFERED_OUTS_PAGEBLOB_CHUNK_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304
    public static final long    BLOB_SIZE_LIMIT = 5L * 1024L * 1024L * 1024L * 1024L; //default is 5TB
}
