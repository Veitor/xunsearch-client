package com.hangjiayun.infrastructure.tokenizer;

import com.hangjiayun.infrastructure.XsDocument;
import com.hangjiayun.infrastructure.XsException;
import com.hangjiayun.infrastructure.XsTokenizer;

import java.util.ArrayList;

/**
 * 内置定长分词器
 */
public class XsTokenizerXlen implements XsTokenizer {
    private int arg = 2;
    public XsTokenizerXlen(String arg) {
        if (!arg.isBlank() && !arg.isEmpty()) {
            this.arg = Integer.parseInt(arg);
            if (this.arg < 1 || this.arg > 255) {
                throw new XsException("Invalid argument for " + this.getClass().getName() + ":" + arg);
            }
        }
    }
    @Override
    public String[] getTokens(String value, XsDocument doc) {
        ArrayList<String> terms = new ArrayList<>();
        //PHP版本是按字节切割的，目前这里简单的按code unit进行切割，后续有问题再调整，一般来说该分词器只适用于单字节字符
        for (int i=0;i<value.length();i+=this.arg) {
            terms.add(value.substring(i, Math.min(i + this.arg + 1, value.length())));
        }
        return terms.toArray(new String[0]);
    }

    @Override
    public String[] getTokens(String value) {
        return this.getTokens(value, null);
    }
}
