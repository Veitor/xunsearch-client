package com.hangjiayun.infrastructure;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class XsDocument {
    private Map<String, String> data = new HashMap<>();
    private String charset;
    private static final int resSize = 20;
    //字段名=>[词语=>权重]
    private Map<String, Map<String, Integer>> terms = new HashMap<>();
    private Map<String, String> texts = new HashMap<>();

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

    /**
     * 重写接口，在文档提交到索引服务器前调用
     * 继承此类进行重写该方法时，必须调用super.beforeSave(index)以确保正确
     * @param index 索引操作对象
     * @return 默认返回true
     */
    public boolean beforeSubmit(XsIndex index) {
        if (this.charset == null) {
            this.charset = index.xs.getDefaultCharset();
        }
        return true;
    }

    /**
     * 获取文档字段的值
     * @param name 字段名称
     * @return 字段值，弱不存在则返回null
     */
    public String f(String name) {
        return this.getField(name);
    }

    /**
     * 获取字段的附加索引词列表（仅限索引文档）
     * @param field 字段名
     * @return 索引词列表（key为词，value为词的权重），若无则返回null
     */
    public Map<String, Integer> getAddTerms(String field) {
        if (this.terms.isEmpty() || !this.terms.containsKey(field)) {
            return null;
        }
        HashMap<String,Integer> terms = new HashMap<>();
        for (Map.Entry<String, Integer> entry : this.terms.get(field).entrySet()) {
            terms.put(this.autoConvert(entry.getKey()), entry.getValue());
        }
        return terms;
    }

    public Map<String, Integer> getAddTerms(XsFieldMeta field) {
        return this.getAddTerms(field.name);
    }

    /**
     * 获取字段的附加索引文本（仅限索引文档）
     * @param field 字段名称
     * @return 文本内容，若无则返回null
     */
    public String getAddIndex(String field) {
        if (this.texts.isEmpty() || !this.texts.containsKey(field)) {
            return null;
        }
        return this.autoConvert(this.texts.get(field));
    }

    public String getAddIndex(XsFieldMeta field) {
        return this.getAddIndex(field.name);
    }

    /**
     * 重写接口，在文档成功提交到索引服务器后调用
     * 继承此类进行重写该方法时，强烈建议要调用super.afterSave(index)以确保完成
     * @param index 索引操作对象
     */
    public void afterSubmit(XsIndex index) {

    }
}
