package com.example.exampledataflow;

import java.io.Serializable;
import java.util.Objects;

public class IncotermEntity implements Serializable {

    private String name;

    private String code;

    private String active;

    public IncotermEntity() {
    }

    public IncotermEntity(String name, String code, String active) {
        this.name = name;
        this.code = code;
        this.active = active;
    }

    @Override
    public String toString() {
        return "{" +
                "- name='" + name + '\'' +
                "- code='" + code + '\'' +
                "- active='" + active + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IncotermEntity that = (IncotermEntity) o;
        return Objects.equals(name, that.name) && Objects.equals(code, that.code) && Objects.equals(active, that.active);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, code, active);
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getActive() {
        return active;
    }

    public void setActive(String active) {
        this.active = active;
    }
}
