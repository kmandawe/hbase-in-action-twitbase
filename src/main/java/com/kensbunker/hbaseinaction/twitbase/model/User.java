package com.kensbunker.hbaseinaction.twitbase.model;

public abstract class User {
    public String user;
    public String name;
    public String email;
    public String password;
    public Long tweetCount;

    @Override
    public String toString() {
        return String.format("<User: %s, %s, %s>", user, name, email);
    }
}
