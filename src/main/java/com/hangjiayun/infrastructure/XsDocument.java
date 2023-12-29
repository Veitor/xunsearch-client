package com.hangjiayun.infrastructure;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class XsDocument {
    private Map<String, String> data = new HashMap<>();
    private String charset;
    private static final int resSize = 20;
    /**
     * 在具体使用时，这些key的值的类型为：
     * - docid: Long类型
     * - ccount: Long类型
     * - percent: Long类型
     * - weight: Float类型
     * - matched: ArrayList<String>类型
     */
    private Map<String, Object> meta = new HashMap<>(5);
    public XsDocument(String p, String d) {
        if (p != null) {
            byte[] pbytes = p.getBytes(StandardCharsets.ISO_8859_1);
            if (pbytes.length != resSize) {
                this.setCharset(p);
                return;
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(pbytes);
            this.meta.put("docid", (long)byteBuffer.getInt());
            this.meta.put("ccount", (long)byteBuffer.getInt());
            this.meta.put("percent", (long)byteBuffer.getInt());
            this.meta.put("weight", byteBuffer.getFloat());
        }

        if (d != null) {
            this.setCharset(d);
        }
    }

    public void setCharset(String charset) {
        this.charset = charset.toUpperCase();
        if (this.charset.equals("UTF8")) {
            this.charset = "UTF-8";
        }
    }

    /**
     * 设置某个字段的值
     * @param name 字段名称
     * @param value 字段值。这里为了做兼容，所有值都视为String字符串类型
     * @param isMeta 是否为元数据字段
     */
    public void setField(String name, Object value, boolean isMeta) {
        if (value == null) {
            if (isMeta) {
                this.meta.remove(name);
            } else {
                this.data.remove(name);
            }
        } else {
            if (isMeta) {
                this.meta.put(name, value);
            } else {
                this.data.put(name, String.valueOf(value));
            }
        }
    }

    public void setField(String name, Object value) {
        setField(name, value, false);
    }

    public Map<String, String> getFields() {
        return this.data;
    }

    /**
     * 代替PHP版本中通过__get魔术方法的实现
     * @return
     */
    public String getField(String field) {
        if (!this.data.containsKey(field)) {
            return null;
        }
        return this.autoConvert(this.data.get(field));
    }

    /**
     * 将迅搜内部用的（即迅搜服务端、客户端通信时的编码）UTF-8编码与指定的文档编码（业务中需要指定使用的编码）按需相互转换
     * @param value
     * @return
     */
    public String autoConvert(String value) {
        if (this.charset == null || this.charset.equals("UTF-8") || value == null || value.matches("[\\x81-\\xFE]")) {
            return value;
        }
        String from = this.meta.isEmpty() ? this.charset : "UTF-8";
        String to = this.meta.isEmpty() ? "UTF-8" : this.charset;
        return Xs.convert(value, to ,from);
    }
}
