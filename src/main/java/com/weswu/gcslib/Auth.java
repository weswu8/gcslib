package main.java.com.weswu.gcslib;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;

public class Auth {
    private Auth(){}
    static Storage newClient(Config config) throws GcsException {
        try {
            final StorageOptions.Builder builder = StorageOptions.newBuilder();
            builder.setProjectId(config.getProjectId());
            builder.setCredentials(GoogleCredentials.fromStream(
                    new FileInputStream(config.getSaFile())));
            final Storage client = builder.build().getService();
            return client;
        }
        catch (StorageException | IOException  e) {
            throw new GcsException(e.getMessage());
        }
    }
}
