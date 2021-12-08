package com.jingsky.mv.maxwell.util;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public class StringInputStream extends ByteArrayInputStream {
    private final String string;

    public StringInputStream(String s) throws UnsupportedEncodingException {
        super(s.getBytes(Charset.forName("UTF-8")));
        this.string = s;
    }

    public String getString() {
        return this.string;
    }
}
