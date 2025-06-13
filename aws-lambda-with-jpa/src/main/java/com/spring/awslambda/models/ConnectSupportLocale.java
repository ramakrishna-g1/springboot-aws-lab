package com.spring.awslambda.models;

import jakarta.persistence.*;

@Entity
@Table(name = "connect_support_locale")
public class ConnectSupportLocale {
    @Id
    @GeneratedValue
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @Column(name = "code")
    private String code;

    @Column(name = "local_name")
    private String localName;

    @Column(name = "user_assistance_name")
    private String userAssistanceName;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getLocalName() {
        return localName;
    }

    public void setLocalName(String localName) {
        this.localName = localName;
    }

    public String getUserAssistanceName() {
        return userAssistanceName;
    }

    public void setUserAssistanceName(String userAssistanceName) {
        this.userAssistanceName = userAssistanceName;
    }
}