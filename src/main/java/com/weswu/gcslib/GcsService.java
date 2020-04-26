package main.java.com.weswu.gcslib;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import java.io.IOException;

public final class GcsService {
    private static GcsService instance;
    private Storage storage;
    public static GcsService getInstance(Config config) throws GcsException {
        if(instance == null){
            synchronized (GcsService.class) {
                if(instance == null){
                    instance = new GcsService(config);
                }
            }
        }
        return instance;
    }
    private GcsService(Config config) throws GcsException {
            this.storage = Auth.newClient(config);
    }

    public final Blob getBlob (GcsReqParmas reqParams) throws GcsException{
        Blob blob = storage.get(BlobId.of(reqParams.getBucket(), reqParams.getBlob()));
        if (blob == null ||!blob.exists()) {
            String errMessage = "Blob: " + reqParams.getBlobFullPath() + " does't exist.";
            throw new GcsException(errMessage);
        }
        return blob;
    }

    public final Blob createBlob(GcsReqParmas reqParams) throws GcsException {
        Blob blob = storage.get(BlobId.of(reqParams.getBucket(), reqParams.getBlob()));
        if (blob == null ||!blob.exists()) {
            blob = storage.create( BlobInfo.newBuilder(BlobId.of(reqParams.getBucket(), reqParams.getBlob()))
                                    .setContentType(reqParams.getContentType())
                                    .build());
        }
        return blob;
    }
}
