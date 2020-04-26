package main.java.com.weswu.gcslib;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class BlobBufferedOus extends OutputStream {
    private Blob blob;
    private WriteChannel writer;
    private GcsReqParmas reqParmas;
    /* the central buffer */
    private byte[] centralBuffer;
    /* the pointer of the central buffer */
    int centralBufOffset= 0;
    int centralBufferSize = Constants.BLOB_BUFFERED_OUTS_BUFFER_SIZE;
    long totalDataWBuffered = 0;
    long totalDataUploaded = 0;
    long localFileSize = 0;
    /* the upload chunk size, the should be smaller than the buffer size */
    int chunkSizeOfBB = Constants.BLOB_BUFFERED_OUTS_CHUNK_SIZE;
    int chunkNumber = 0;
    /* the path of the blob */
    String fullBlobPath;
    /* the flag represent the state of local stream */
    boolean isBlobClosed = false;
    Integer numOfCommitedBlocks = null;

    public BlobBufferedOus(Config config, GcsReqParmas reqParams) throws GcsException, IOException {
        this.blob = GcsService.getInstance(config).createBlob(reqParams);
        this.writer = this.blob.writer();
        this.reqParmas = reqParams;
        this.fullBlobPath = reqParams.getBlobFullPath();
        this.centralBuffer = new byte[centralBufferSize];
        this.localFileSize = (null != reqParams.getLocalFileSize()) ? reqParams.getLocalFileSize() : 0;
    }

    public Blob getBlob() {
        return blob;
    }

    @Override
    public void write(int b) throws GcsException {
        /* simply call the write function */
        byte[] oneByte = new byte[1];
        oneByte[0] = (byte) b;
        write(oneByte, 0, 1);
    }
    @Override
    public synchronized void write(final byte[] data, final int offset, final int length) throws GcsException {
        /* throw the parameters error */
        if (offset < 0 || length < 0 || length > data.length - offset) {
            throw new GcsException(new IndexOutOfBoundsException());
        }
        /* test the upload contions */
        verifyUploadConditions();

        /* write data to the buffer */
        writeToBuffer(data, offset, length);
        /* check the buffered data and the chunk size threshold */
        if (isBufferedDataReadyToUpload()){
            int numOfdataUploaded = 0;
            if ((numOfdataUploaded = uploadBlobChunk(centralBuffer, 0, centralBufOffset)) > 0){
                totalDataUploaded += numOfdataUploaded;
                /* clean the buffer */
                byte[] tempBuffer = new byte[centralBufferSize];
                centralBuffer = tempBuffer;
                /* reset the chunk count */
                centralBufOffset = 0;
            }
        }
    }
    /* write line function */
    public void writeLine(String line) throws GcsException {
        /* simply call the write function */
        byte[] lineBytes = new byte[0];
        try {
            lineBytes = (line + System.getProperty("line.separator")).getBytes(reqParmas.getCharset());
            write(lineBytes, 0, lineBytes.length);
        } catch (UnsupportedEncodingException e) {
           throw new GcsException(e);
        }

    }
    /* push the data from buffer to blob */
    public synchronized final void flush() throws GcsException {
        try {
            if (centralBufOffset > 0) {
                int numOfdataUploaded = 0;
                if ((numOfdataUploaded = uploadBlobChunk(centralBuffer, 0, centralBufOffset)) > 0) {
                    totalDataUploaded += numOfdataUploaded;
                    /* reset the chunk count */
                    centralBufOffset = 0;
                }
            }
        } catch (Exception ex) {
            String errMessage = "Unexpected exception occurred when flush to buffered data to the blob: " + fullBlobPath + ". " + ex.getMessage();
            throw new GcsException(ex.getMessage());
        }
    }
    @SuppressWarnings("static-access")
    @Override
    public synchronized final void close() throws GcsException {
        try {
            if (isBlobClosed) {
                return;
            }
            /* flush the data */
            flush();
            /* clean the buffer */
            centralBuffer = null;
            /* close the write channel */
            this.writer.close();
            //}
            System.out.println("Closed the blob output stream.");
        } catch (Exception ex) {
            String errMessage = "Unexpected exception occurred when closing the blob output stream " + fullBlobPath + ". " + ex.getMessage();
            throw  new GcsException(ex.getMessage());
        }finally{
            isBlobClosed = true;
        }

    }
    /* write data to buffer */
    public synchronized final int writeToBuffer(byte[] rawData, int offset, int length){
        int numOfDataWrited = 0;
        /* the capacity of central buffer is ok */
        if ((centralBuffer.length - centralBufOffset) > length ){
            System.arraycopy(rawData, offset, centralBuffer, centralBufOffset, length);
        } else {
            byte[] tempBuffer = new byte[centralBufOffset + length];
            System.arraycopy(centralBuffer, 0, tempBuffer, 0, centralBufOffset);
            System.arraycopy(rawData, offset, tempBuffer, centralBufOffset, length);
            centralBuffer = tempBuffer;
        }
        numOfDataWrited = length;
        centralBufOffset += numOfDataWrited;
        totalDataWBuffered += numOfDataWrited;
        return numOfDataWrited;
    }
    /* upload a chunk of data from to blob */
    @SuppressWarnings("static-access")
    public synchronized final int uploadBlobChunk (byte[] rawData,  int offset, int length) throws GcsException {
        int dataUploadedThisChunk = 0;
        try {
            ByteArrayInputStream bInput = new ByteArrayInputStream(rawData, offset, length);
            /* update the chunk counter */
            chunkNumber ++;
            this.writer.write(ByteBuffer.wrap(rawData, 0, length));
            dataUploadedThisChunk = length;

        } catch (IOException ex) {
            String errMessage = "Unexpected exception occurred when uploading to the blob : "
                    + this.fullBlobPath + ", No. of chunk: " + chunkNumber + "." + ex.getMessage();
            throw new GcsException(errMessage);
        }
        System.out.println(String.format("Uploading to %s , blockId: %d, size: %d", fullBlobPath, chunkNumber, length));
        return dataUploadedThisChunk;
    }

    public synchronized boolean isBufferedDataReadyToUpload() {
        boolean result = false;
        if (centralBufOffset > chunkSizeOfBB){ result = true;}
        return result;
    }

    public synchronized void verifyUploadConditions() throws GcsException {
        long blobSizeLimit = 0;
        blobSizeLimit = Constants.BLOB_SIZE_LIMIT;
        if (null == numOfCommitedBlocks){numOfCommitedBlocks = 0;}
        /* verify the size of local file is under the limit */
        if (localFileSize > blobSizeLimit){
            String errMessage = "The size of the source file exceeds the size limit: " + blobSizeLimit + ".";
            throw new GcsException(errMessage);
        }
        return;
    }
}