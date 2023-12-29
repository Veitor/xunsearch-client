package com.hangjiayun.infrastructure;

public class XsException extends RuntimeException{
    public int code;
    private final StackTraceElement[] stackTraceElements = this.getStackTrace();

    public XsException(String message, int code, Throwable throwable) {
        super(message, throwable);
        this.code = code;
    }

    public XsException(String message) {
        this(message, 0, null);
    }

    public XsException(String message, int code) {
        this(message, code, null);
    }

    public XsException(String message, Throwable throwable) {
        this(message, 0, throwable);
    }

    @Override
    public String toString() {
//        String string = "[" + this.getClass().getSimpleName() + "] " + getRelPath(getFile()) + "(" +  + "): ";
        String string = super.toString();
        string += getMessage() + (getCode() > 0 ? "(S#" + getCode() + ")" : "");
        return string;
    }

    public int getCode() {
        return code;
    }
}
