package com.hangjiayun.infrastructure.tokenizer;

import com.hangjiayun.infrastructure.XsDocument;
import com.hangjiayun.infrastructure.XsTokenizer;

/**
 * 内置空分词器
 */
public class XsTokenizerNone implements XsTokenizer {
    @Override
    public String[] getTokens(String value, XsDocument doc) {
        return new String[0];
    }

    @Override
    public String[] getTokens(String value) {
        return getTokens(value, null);
    }
}
