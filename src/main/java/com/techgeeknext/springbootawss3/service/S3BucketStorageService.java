package com.techgeeknext.springbootawss3.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Service
public class S3BucketStorageService {

    private Logger logger = LoggerFactory.getLogger(S3BucketStorageService.class);

    @Autowired
    private AmazonS3 amazonS3Client;

    @Value("${application.bucket.name}")
    private String bucketName;

    /**
     * Upload file into AWS S3
     *
     * @param keyName
     * @param file
     * @return String
     */
    public String uploadFile(String keyName, MultipartFile file) {
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(file.getSize());
            amazonS3Client.putObject(bucketName, keyName, file.getInputStream(), metadata);
            return "File uploaded: " + keyName;
        } catch (IOException ioe) {
            logger.error("IOException: " + ioe.getMessage());
        } catch (AmazonServiceException serviceException) {
            logger.info("AmazonServiceException: "+ serviceException.getMessage());
            throw serviceException;
        } catch (AmazonClientException clientException) {
            logger.info("AmazonClientException Message: " + clientException.getMessage());
            throw clientException;
        }
        return "File not uploaded: " + keyName;
    }

    /**
     * Deletes file from AWS S3 bucket
     *
     * @param fileName
     * @return
     */
    public String deleteFile(final String fileName) {
        amazonS3Client.deleteObject(bucketName, fileName);
        return "Deleted File: " + fileName;
    }


    /**
     * Downloads file using amazon S3 client from S3 bucket
     *
     * @param keyName
     * @return ByteArrayOutputStream
     */
    public FileOutputStream downloadFile(String keyName) {
        try {
            S3Object o = amazonS3Client.getObject(bucketName, keyName);
            S3ObjectInputStream s3is = o.getObjectContent();
            BufferedReader bal = new BufferedReader(new InputStreamReader(s3is));
            //File fos = new File("/Users/macair/Output1.csv");

            BufferedWriter wri = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/macair/Output1.csv"))/*new FileWriter(fos)*/);
            String line = null;
            while((line = bal.readLine())!=null){
                wri.write(line+"\n");
                wri.flush();
            }
            //this.printTheInput(s3is.getDelegateStream());
           /* FileOutputStream fos = new FileOutputStream(new File("/Users/macair/Output1.csv"));
            byte[] read_buf = new byte[1024*1024];
            int read_len = 0;
            System.out.println(s3is.read(read_buf));
            while ((s3is.read(read_buf)) != -1) {

                fos.write(read_buf);
            }*/
            bal.close();
            wri.close();

            s3is.close();

            //////////////////////
            /*S3Object s3object = amazonS3Client.getObject(new GetObjectRequest(bucketName, keyName));

            InputStream is = s3object.getObjectContent();
            FileOutputStream fileOutputStream = new FileOutputStream(keyName);
            //ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            int len;
            byte[] buffer = new byte[4096];
            while ((len = is.read(buffer, 0, buffer.length)) != -1) {
                fileOutputStream.write(buffer, 0, len);
            }
            System.out.print("Data using toByteArray(): ");
            for(int i=0; i<buffer.length; i++) {
                System.out.print((char)buffer[i]);
             }*/



        } catch (IOException ioException) {
            logger.error("IOException: " + ioException.getMessage());
        } catch (AmazonServiceException serviceException) {
            logger.info("AmazonServiceException Message:    " + serviceException.getMessage());
            throw serviceException;
        } catch (AmazonClientException clientException) {
            logger.info("AmazonClientException Message: " + clientException.getMessage());
            throw clientException;
        }

        return null;
    }

    private void printTheInput(InputStream delegateStream) {

        try{
            BufferedInputStream bin = new BufferedInputStream(delegateStream);

            // illustrating available method
            System.out.println("Number of remaining bytes:" +
                    bin.available());

            // illustrating markSupported() and mark() method
            boolean b=bin.markSupported();
            if (b)
                bin.mark(bin.available());

            // illustrating skip method
            /*Original File content:
             * This is my first line
             * This is my second line*/
            bin.skip(4);
            System.out.println("FileContents :");

            // read characters from FileInputStream and
            // write them
            int ch;
            while ((ch=bin.read()) != -1)
                System.out.print((char)ch);

            // illustrating reset() method
            bin.reset();
            while ((ch=bin.read()) != -1)
                System.out.print((char)ch);








/*
            String originalString = RandomStringUtils.randomAlphabetic(8);
            InputStream inputStream = new ByteArrayInputStream(originalString.getBytes());

            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int nRead;
            byte[] data = new byte[1024];
            while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }

            buffer.flush();
            byte[] byteArray = buffer.toByteArray();
            for(int i=0; i<byteArray.length; i++) {
                System.out.print((char)byteArray[i]);
            }

            String text = new String(byteArray, StandardCharsets.UTF_8);
            System.out.println(text);*/
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get all files from S3 bucket
     *
     * @return
     */
    public List<String> listFiles() {

        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest()
                        .withBucketName(bucketName);

        List<String> keys = new ArrayList<>();

        ObjectListing objects = amazonS3Client.listObjects(listObjectsRequest);

        while (true) {
            List<S3ObjectSummary> objectSummaries = objects.getObjectSummaries();
            if (objectSummaries.size() < 1) {
                break;
            }

            for (S3ObjectSummary item : objectSummaries) {
                if (!item.getKey().endsWith("/"))
                    keys.add(item.getKey());
            }

            objects = amazonS3Client.listNextBatchOfObjects(objects);
        }

        return keys;
    }

}
