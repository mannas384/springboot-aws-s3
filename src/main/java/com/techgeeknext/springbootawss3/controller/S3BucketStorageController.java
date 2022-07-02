package com.techgeeknext.springbootawss3.controller;

import com.techgeeknext.springbootawss3.service.S3BucketStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.util.List;

@RestController
public class S3BucketStorageController {
    @Autowired
    S3BucketStorageService service;

    @GetMapping("/list/files")
    public ResponseEntity<List<String>> getListOfFiles() {
        return new ResponseEntity<>(service.listFiles(), HttpStatus.OK);
    }

    @PostMapping("/file/upload")
    public ResponseEntity<String> uploadFile(@RequestParam("fileName") String fileName,
                                             @RequestParam("file") MultipartFile file) {
        return new ResponseEntity<>(service.uploadFile(fileName, file), HttpStatus.OK);
    }

    @GetMapping(value = "/download/{filename}")
    public ResponseEntity<String> downloadFile(@PathVariable String filename) {
        FileOutputStream downloadInputStream = service.downloadFile(filename);

        System.out.println(downloadInputStream.toString());
        /*byte[] byteData = downloadInputStream.toByteArray();
        System.out.print("Data using toByteArray(): ");
        for(int i=0; i<byteData.length; i++) {
            System.out.print((char)byteData[i]);
        }*/

        return ResponseEntity.ok()
                .contentType(contentType(filename))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .body(downloadInputStream.toString());
    }

    @GetMapping(value = "/delete/{filename}")
    public ResponseEntity<String> deleteFile(@PathVariable("filename") String filename) {
        return new ResponseEntity<>(service.deleteFile(filename), HttpStatus.OK);
    }

    private MediaType contentType(String filename) {
        String[] fileArrSplit = filename.split("\\.");
        String fileExtension = fileArrSplit[fileArrSplit.length - 1];
        switch (fileExtension) {
            case "txt":
                return MediaType.TEXT_PLAIN;
            case "png":
                return MediaType.IMAGE_PNG;
            case "jpg":
                return MediaType.IMAGE_JPEG;
            default:
                return MediaType.APPLICATION_OCTET_STREAM;
        }
    }
}
