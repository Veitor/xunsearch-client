package com.hangjiayun.infrastructure;

public interface XsTokenizer {
    public static final String DFL = "0";

    /**
     * 执行分词并返回词列表
     * @param value 待分词的字段值（UTF-8编码）
     * @param doc 当前相关的索引文档
     * @return 切好的词组成的数组
     */
    String[] getTokens(String value, XsDocument doc);
    String[] getTokens(String value);
}
