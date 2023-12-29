package com.hangjiayun.infrastructure.tokenizer;

import com.hangjiayun.infrastructure.XsDocument;
import com.hangjiayun.infrastructure.XsTokenizer;

/**
 * 内置的分割分词器
 */
public class XsTokenizerSplit implements XsTokenizer {
    private String arg = "";
    public XsTokenizerSplit(String arg) {
        if (arg != null && !arg.isBlank()) {
            this.arg = arg;
        }
    }
    public XsTokenizerSplit() {
        this(null);
    }
    @Override
    public String[] getTokens(String value, XsDocument doc) {
        //todo: 这里暂时少实现一个支持正则的分割
        return value.split(this.arg);
    }

    @Override
    public String[] getTokens(String value) {
        return getTokens(value, null);
    }
}
