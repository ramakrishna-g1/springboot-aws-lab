package com.spring.aws_s3.model;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "upload_tracker")
@EntityListeners({AuditingEntityListener.class})
public class UploadTracker {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id")
    private long id;

    /**
     * Name of file to be uploaded
     */
    @Column(name = "file_name", nullable = false, columnDefinition = "varchar(255)")
    private String fileName;

    /**
     * Path to the file till parent directory
     */
    @Column(name = "file_path", nullable = false, columnDefinition = "varchar(255)")
    private String filePath;

    /**
     * File Upload Status
     */
    @Column(name = "file_status", columnDefinition = "varchar(20) default 'pending'")
    private String fileStatus;

    /**
     * Number of Retries Done
     */
    @Column(name = "retries_done", columnDefinition = "Integer default 0", length = 1)
    private int retriesDone;

    /**
     * The created date.
     */
    @Column(name = "created_date")
    @CreatedDate
    private Date createdDate;

    /**
     * The modified date.
     */
    @Column(name = "modified_date")
    @LastModifiedDate
    private Date modifiedDate;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileStatus() {
        return fileStatus;
    }

    public void setFileStatus(String fileStatus) {
        this.fileStatus = fileStatus;
    }

    public int getRetriesDone() {
        return retriesDone;
    }

    public void setRetriesDone(int retriesDone) {
        this.retriesDone = retriesDone;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public Date getModifiedDate() {
        return modifiedDate;
    }

    public void setModifiedDate(Date modifiedDate) {
        this.modifiedDate = modifiedDate;
    }

    @Override
    public String toString() {
        return "UploadTracker{" +
                "id=" + id +
                ", fileName='" + fileName + '\'' +
                ", status='" + fileStatus + '\'' +
                ", retriesDone=" + retriesDone +
                ", createdDate=" + createdDate +
                ", modifiedDate=" + modifiedDate +
                '}';
    }

}
