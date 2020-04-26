package main.java.com.weswu.gcslib;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BlobBufferedIns extends InputStream {
    private Blob blob;
    /* the central buffer */
    private byte[] centralBuffer = new byte[]{};
    /* the pointer of the central buffer */
    private int centralBufOffset = 0;
    /* the available bytes in central buffer */
    private int numOfBtsAalInCentralBuf = 0;
    private long blobSize = -1;
    private long blobOffset = 0;
    private long numOfBlobBtsLeft = 0;
    private int dwLocalBufferSize = 0;
    private String fullBlobPath;
    /* count used by readline function */
    private long readOffset = 0;

    private boolean isBlobEOF = false;

    @SuppressWarnings("static-access")
    public BlobBufferedIns(Config config, GcsReqParmas reqParams) throws GcsException {
        this.blob = GcsService.getInstance(config).getBlob(reqParams);
        this.blobSize = this.blob.getSize();
        this.numOfBlobBtsLeft = this.blobSize;
        this.fullBlobPath = reqParams.getBlobFullPath();
        this.dwLocalBufferSize = (int) Math.min(Constants.BLOB_BUFFERED_INS_DOWNLOAD_SIZE * 2, this.blobSize);
    }
    public Blob getBlob() {
        return blob;
    }

    @Override
    public synchronized int read(byte[] outputBuffer, int offset, int len) throws GcsException {
        int numOfBytesReaded = 0;
        /* for multiple threads, When the subsequent thread runs slower than the previous multiple threads, the read offset
         * of the slower old thread may small than the global read offset. so we need reset the global offset. this may cause
         * download the same data multiple times.
         */
        boolean resetFlag = false;
        /* the index of the offset for the read operation */
        int numOfBtsSubBatchDwed= 0;
        /* the skipped bytes for read */
        int numOfBtsSkiped = 0;
        /* If len is zero return 0 per the InputStream contract */
        if (len == 0) {
            return 0;
        }
        /* clean the output buffer, avoid dirty data */
        Arrays.fill(outputBuffer, (byte) 0);

        /* check the offset */
        numOfBtsSkiped = (int) (offset - readOffset);
        System.out.println(String.format("numOfBtsSkiped %d, readOffset %d" , numOfBtsSkiped, readOffset));
        /* the offset may decrease in some condition, such as for tail command
         * in this condition, we should reset all the global variable
         */
        if (numOfBtsSkiped < 0){
            resetGlobalVariables(offset);
            resetFlag = true;
            /* reset this variable again */
            numOfBtsSkiped = 0;
        }
        numOfBtsAalInCentralBuf = ((numOfBtsAalInCentralBuf - numOfBtsSkiped) > 0) ? (numOfBtsAalInCentralBuf - numOfBtsSkiped) : 0;

        /* read buffer if buffered data is enough */
        if (numOfBtsAalInCentralBuf >= len){
            byte[] btsReadedTempBuf = readFromBuffer(numOfBtsSkiped, len);
            numOfBytesReaded = btsReadedTempBuf.length;
            System.arraycopy(btsReadedTempBuf, 0, outputBuffer, 0, numOfBytesReaded);
            readOffset += numOfBytesReaded + numOfBtsSkiped;
            return numOfBytesReaded;
        }
        /* if stream is closed */
        if (isBlobEOF & !resetFlag) {
            /* reach the end of the buffer */
            if (numOfBtsAalInCentralBuf == 0){
                return -1;
            }
            byte[] btsReadedTempBuf = readFromBuffer(numOfBtsSkiped, numOfBtsAalInCentralBuf);
            numOfBytesReaded = btsReadedTempBuf.length;
            System.arraycopy(btsReadedTempBuf, 0, outputBuffer, 0, numOfBytesReaded);
            readOffset += numOfBytesReaded + numOfBtsSkiped;
            return numOfBytesReaded;
        }
        /* numOfBtsSkiped should be used only one times in the loop */
        int numOfBtsSkipedInLoop = numOfBtsSkiped;
        /* download new data in chunks and consume it */
        while (numOfBtsAalInCentralBuf < len){
            if ((this.isBlobEOF && !resetFlag) || blobSize - offset <= 0 ){ break; }
            /* avoid out of boundary */
            long bytesToDown = (int) Math.min(dwLocalBufferSize, blobSize - offset);
            int numOfBytesDwed = updateCentralBuffer(numOfBtsSkipedInLoop, offset + numOfBtsSubBatchDwed, (int) bytesToDown);
            /* reset to zero, otherwise this will cause the error for sequenced steps */
            numOfBtsSkipedInLoop = 0;
            numOfBtsSubBatchDwed += numOfBytesDwed;
        }
        /* read the data from bytesUnreadedInBuffer and store in outputBuffer */
        len =  Math.min(numOfBtsAalInCentralBuf, len);
        byte[] btsReadedTempBuf = readFromBuffer(0, len);
        numOfBytesReaded = btsReadedTempBuf.length;
        System.arraycopy(btsReadedTempBuf, 0, outputBuffer, 0, numOfBytesReaded);
        readOffset += numOfBytesReaded + numOfBtsSkiped;
        return numOfBytesReaded;
    }

    @Override
    public int read() throws IOException {
        byte[] oneByte = new byte[1];
        int result = read(oneByte, (int)readOffset, oneByte.length);
        if (result <= 0) {
            return -1;
        }
        return oneByte[0];
    }
    /* Read a \r\n terminated line from an InputStream.
     * @return line without the newline or empty String if InputStream is empty
     */
    public String readLine() throws IOException {
        StringBuilder builder = new StringBuilder();
        /* readOffset is updated in read(), reach the end of file normally */
        if (readOffset == blobSize){
            return null;
        }
        while (true) {
            int ch = read();
            if (ch == '\r') {
                ch = read();
                if (ch == '\n') {
                    break;
                } else {
                    throw new IOException("unexpected char after \\r: " + ch);
                }
            } else if (ch == -1) {
                if (builder.length() > 0) {
                    return builder.toString();
                    //throw new IOException("unexpected end of stream");
                }else{
                    return null;
                }
            }
            builder.append((char) ch);
        }
        return builder.toString();
    }
    /* reset all the index to zero, specially reset the  read offset as the new value */
    private synchronized final void resetGlobalVariables(int readOffset){
        this.blobOffset = 0;
        this.numOfBlobBtsLeft = 0;
        this.centralBuffer = new byte[]{};
        this.centralBufOffset = 0;
        this.numOfBtsAalInCentralBuf = 0;
        this.readOffset = readOffset;
        this.isBlobEOF = false;
    }
    /**
     * update the central buffer
     * @param len
     * @return the number of bytes added to the central buffer
     * @throws GcsException
     */
    private synchronized final int updateCentralBuffer(int bufferOffset, int dwOffset, int len) throws GcsException {
        int numOfBytesDwed = 0;
        byte[] bytesDwedBuf = downloadBlobChunk(dwOffset, len);
        if (null != bytesDwedBuf && bytesDwedBuf.length > 0){
            byte[] bytesTotalDwedBuf = new byte[numOfBtsAalInCentralBuf + bytesDwedBuf.length];
            /* the available data in central buffer */
            if (numOfBtsAalInCentralBuf > 0){
                /* update the central buffer offset */
                centralBufOffset += bufferOffset;
                /* copy data in buffer first */
                System.arraycopy(centralBuffer, centralBufOffset,
                        bytesTotalDwedBuf, 0, numOfBtsAalInCentralBuf);
            }
            /* copy the new downloaded data */
            System.arraycopy(bytesDwedBuf, 0,
                    bytesTotalDwedBuf, numOfBtsAalInCentralBuf, bytesDwedBuf.length);
            /* refresh the bytesUnreadedInBuffer pointer */
            centralBuffer = bytesTotalDwedBuf;
            /* reset the offset*/
            centralBufOffset = 0;
            numOfBtsAalInCentralBuf = centralBuffer.length - centralBufOffset;
            numOfBytesDwed = bytesDwedBuf.length;
        }else{
            this.isBlobEOF = true;
        }
        return numOfBytesDwed;
    }

    /* download a chunk of data from the blob */
    private synchronized final byte[] downloadBlobChunk (long offset, int len) throws GcsException {
        long bytesDownloaded = 0;
        ByteBuffer dwLocalBuffer = null;
        if (isBlobEOF){	return null;}
        /* how much to read (only last chunk may be smaller) */
        len = Math.min((int)(blobSize - offset), len) ;
        try (ReadChannel reader = blob.reader()) {
            reader.seek(offset);
            dwLocalBuffer = ByteBuffer.allocate(len);
            bytesDownloaded = reader.read(dwLocalBuffer);
        }catch (IOException e){
            throw new GcsException(e);
        }
        numOfBlobBtsLeft = blobSize - offset - bytesDownloaded;
        blobOffset = (offset + bytesDownloaded);
        if (numOfBlobBtsLeft <= 0){this.isBlobEOF = true;}
        System.out.println(String.format("Downloaded from %s , size of this chunk : %d, total downloaded size: %d", fullBlobPath, bytesDownloaded, blobOffset));
        return dwLocalBuffer.array();

    }
    /* read the date from the buffer */
    private synchronized final  byte[] readFromBuffer (int offset, int numOfbytesToRead){
        byte[] chunkBytesReaded;
        /* update the offset of the buffer */
        centralBufOffset += offset;
        if (numOfBtsAalInCentralBuf <= numOfbytesToRead){
            chunkBytesReaded = new byte[numOfBtsAalInCentralBuf];
        }else{
            chunkBytesReaded = new byte[numOfbytesToRead];
        }
        System.arraycopy(centralBuffer, centralBufOffset, chunkBytesReaded, 0, numOfbytesToRead);
        centralBufOffset += chunkBytesReaded.length;
        numOfBtsAalInCentralBuf = centralBuffer.length - centralBufOffset;
        return chunkBytesReaded;
    }

}
