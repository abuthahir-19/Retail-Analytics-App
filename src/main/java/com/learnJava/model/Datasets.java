package com.learnJava.model;

public class Datasets {
    public String name;
    public String path;
    public String format;

    public Datasets (String name, String path, String format) {
        this.name = name;
        this.path = path;
        this.format = format;
    }

    public String getName () {
        return this.name;
    }

    public String path () {
        return this.path;
    }

    public String format () {
        return this.format;
    }
}
