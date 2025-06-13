package com.spring.aws_s3.model;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import javax.persistence.*;
import java.time.OffsetDateTime;

@Entity
@EntityListeners(value = {AuditingEntityListener.class})
@Table(name = "upload_stream_tracker")
public class UploadStreamTracker {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;

    private String runId;

    private String fileId;

    @Column(length = 700)
    private String hrefContent;

    private String contentType;

    private String status;

    private Long size;

    private String path;

    private String eTag;

    private int retriesDone;

    @CreationTimestamp
    @Column(updatable = false)
    private OffsetDateTime dateCreated;

    @UpdateTimestamp
    private OffsetDateTime dateModified;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public String getHrefContent() {
        return hrefContent;
    }

    public void setHrefContent(String hrefContent) {
        this.hrefContent = hrefContent;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getETag() {
        return eTag;
    }

    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    public int getRetriesDone() {
        return retriesDone;
    }

    public void setRetriesDone(int retriesDone) {
        this.retriesDone = retriesDone;
    }

    public OffsetDateTime getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(OffsetDateTime dateCreated) {
        this.dateCreated = dateCreated;
    }

    public OffsetDateTime getDateModified() {
        return dateModified;
    }

    public void setDateModified(OffsetDateTime dateModified) {
        this.dateModified = dateModified;
    }
}
