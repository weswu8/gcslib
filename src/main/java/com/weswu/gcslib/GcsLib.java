package main.java.com.weswu.gcslib;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class GcsLib {
    public static void main(String[] args){
        String projectId = "xx";
        String saFile = "gcp_redq_sa.json";
        Config config = new Config(projectId, saFile);
        GcsReqParmas insParams = new GcsReqParmas();
        insParams.setBucket("xx.appspot.com");
        insParams.setBlob("trader-2020-03-25.log");
        GcsReqParmas outParams = new GcsReqParmas();
        outParams.setBucket("xx.appspot.com");
        outParams.setBlob("trader-2020-03-26.log");
        try {
            BlobBufferedIns bbIns = new BlobBufferedIns(config, insParams);
//            BlobBufferedOus bbOus = new BlobBufferedOus(config, outParams);
            String line;
            while ((line = bbIns.readLine()) != null)
            {
                System.out.println((line));
//                bbOus.writeLine(line);
                Thread.sleep(100);
            }
            bbIns.close();
//            bbOus.close();


            BlobBufferedOus bbOus = new BlobBufferedOus(config, outParams);
//            String outLine = "insert new line here";
//            bbOus.writeLine(outLine);
//            bbOus.close();
            byte[] buffer = new byte[1024];
            try (InputStream input = Files.newInputStream(Paths.get("/Users/weswu/Downloads/AutoTrader/DeepLearning-Capsnet-Timeseries-MNIST-master.zip"))) {
                while ((input.read(buffer)) >= 0) {
                    try {
                        bbOus.write(buffer);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
                bbOus.close();
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
