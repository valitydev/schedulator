package dev.vality.schedulator.backoff;

public interface JobBackOff {

    long nextBackOff();

}
