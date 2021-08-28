package com.kensbunker.hbaseinaction.twitbase.model;

import org.joda.time.DateTime;

public abstract class Twit {

  public String user;
  public DateTime dt;
  public String text;

  @Override
  public String toString() {
    return String.format("<Twit: %s %s %s>", user, dt, text);
  }
}
