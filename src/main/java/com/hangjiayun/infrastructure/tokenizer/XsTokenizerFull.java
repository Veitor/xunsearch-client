package com.hangjiayun.infrastructure.tokenizer;

import com.hangjiayun.infrastructure.XsDocument;
import com.hangjiayun.infrastructure.XsTokenizer;

/**
 * 内置整值分词器
 */
public class XsTokenizerFull implements XsTokenizer {
    @Override
    public String[] getTokens(String value, XsDocument doc) {
        return new String[]{value};
    }

    @Override
    public String[] getTokens(String value) {
        return getTokens(value, null);
    }
}
