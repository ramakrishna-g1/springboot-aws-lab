package com.spring.aws_s3.dto;

import java.sql.Timestamp;

public class StreamFileDTO {

    private String Id;

    private String Href;

    private String HrefContent;

    private String Name;

    private String ContentType;

    private Long Size;

    private String Path;

    private Boolean IsArchived;

    private Timestamp DateCreated;

    private Timestamp DateModified;

    private String ETag;

    public String getId() {
        return Id;
    }

    public void setId(String id) {
        Id = id;
    }

    public String getHref() {
        return Href;
    }

    public void setHref(String href) {
        Href = href;
    }

    public String getHrefContent() {
        return HrefContent;
    }

    public void setHrefContent(String hrefContent) {
        HrefContent = hrefContent;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getContentType() {
        return ContentType;
    }

    public void setContentType(String contentType) {
        ContentType = contentType;
    }

    public Long getSize() {
        return Size;
    }

    public void setSize(Long size) {
        Size = size;
    }

    public String getPath() {
        return Path;
    }

    public void setPath(String path) {
        Path = path;
    }

    public Boolean getArchived() {
        return IsArchived;
    }

    public void setArchived(Boolean archived) {
        IsArchived = archived;
    }

    public Timestamp getDateCreated() {
        return DateCreated;
    }

    public void setDateCreated(Timestamp dateCreated) {
        DateCreated = dateCreated;
    }

    public Timestamp getDateModified() {
        return DateModified;
    }

    public void setDateModified(Timestamp dateModified) {
        DateModified = dateModified;
    }

    public String getETag() {
        return ETag;
    }

    public void setETag(String ETag) {
        this.ETag = ETag;
    }
}
