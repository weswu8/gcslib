package main.java.com.weswu.gcslib;

import com.google.cloud.storage.Blob;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GcsReqParmas {
    String bucket;
    String blob;
    Blob blobInstance;
    String destBucket;
    String destBlob;
    String localTmpDir;
    String localDir;
    String localFile;
    String content;
    Long localFileSize;
    boolean doForoce;
    Long blobSize;
    long offset;
    long length;
    String charset;
    String contentType;

    public String getBlobFullPath(){
        return bucket + Constants.PATH_DELIMITER + blob;
    }

    public String getDestBlobFullPath(){
        return destBucket + Constants.PATH_DELIMITER + destBlob;
    }

    public String getLocalFileFullPath(){
        //return localDir + Constants.PATH_DELIMITER + localFile;
        return localDir + localFile;
    }

    public String getCharset(){
        String charset = ((null == this.charset)) ? Constants.DEFAULT_CHARSET : this.charset;
        return charset;
    }

    public String getContentType(){
        String contentType = ((null == this.contentType)) ? Constants.DEFAULT_CONTENT_TYPE : this.contentType;
        return contentType;
    }
}
