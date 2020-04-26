GcsLib
=====
GcsLib is the libraries of the Google cloud storage, The main purpose is to support the transfer of large files.


## Project Goals
support read and write large file


## Features and Updates:
* New extension class of the InputStream and OutputStream. add a new cache layer and is optimized for the blob read and write.
* The contents are pre-cached by chunks when there is read operation. This will eliminate the times of http request and increase the performance greatly. 
* resumeable uploads are used for the write operation. Data is buffered firstly and then be uploaded if the buffer size exceed the threshold. This also can eliminate the times of http request and increase the performance greatly. 

## How To use
### 1.include the gcslib in your project, initialize the config
	Config config = new Config("gcp_project_id", "gcp_service_account_json_file");
### 2.read from the blob
	GcsReqParmas insParams = new GcsReqParmas();
    insParams.setBucket("xxxxx.appspot.com");
    insParams.setBlob("xxxx-2020-03-25.log");
	BlobBufferedIns bbIns = new BlobBufferedIns(insParams);
	String line;
	while ((line = bbIns.readLine()) != null)
	{
		System.out.println((line));
		Thread.sleep(100);
	}
	bbIns.close()
### 3.Write to the blob
	GcsReqParmas ousParams = new GcsReqParmas();
    ousParams.setBucket("xxxxx.appspot.com");
    ousParams.setBlob("xxxx-2020-03-25.log");
    BlobBufferedOus bbOus = new BlobBufferedOus(config, outParams);
    String outLine = "insert new line here";
    bbOus.writeLine(outLine);
    bbOus.close();

### 4.please find other functions from the source code.
		

## Performance Test
* The performance depends on the machine and the network. 

## Dependency
* [google-cloud-storage] Google Cloud Storage Library for Java .

## Limitation and known issues:
* no file verification

## Supported platforms
* Linux
* MacOS
* windows

## License
	Copyright (C) 2017 Wesley Wu jie1975.wu@gmail.com
	This code is licensed under The General Public License version 3
	
## FeedBack
	Your feedbacks are highly appreciated! :)
