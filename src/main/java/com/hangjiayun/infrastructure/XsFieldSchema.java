package com.hangjiayun.infrastructure;

import java.util.HashMap;
import java.util.Map;

public class XsFieldSchema {
    public static final int MIXED_VNO = 255;

    private final Map<String, XsFieldMeta> fields = new HashMap<>();
    private final Map<XsFieldMeta.TYPE, String> typeMap = new HashMap<>();
    private final Map<Integer, String> vnoMap = new HashMap<>();

    public void addField(String field, Map<String, String> config) {
        this.addField(new XsFieldMeta(field, config), config);
    }

    public void addField(XsFieldMeta field, Map<String, String> config) {
        if (this.fields.containsKey(field.name)) {
            throw new RuntimeException("Duplicated field name: `" + field.name + "`");
        }
        if (field.isSpecial()) {
            if (this.typeMap.containsKey(field.type)) {
                String prev = this.typeMap.get(field.type);
                throw new RuntimeException("Duplicated " + config.get("type").toUpperCase() + " field: `" + field.name + "` and `" + prev + "`");
            }
            this.typeMap.put(field.type, field.name);
        }
        field.vno = (field.type == XsFieldMeta.TYPE.BODY) ? MIXED_VNO : this.vnoMap.size();
        this.vnoMap.put(field.vno, field.name);
        if (field.type == XsFieldMeta.TYPE.ID) {
            if (!this.fields.containsKey(field.name)) {
                this.fields.put(field.name, field);
            }
        } else {
            this.fields.put(field.name, field);
        }
    }

    /**
     * 获取项目字段元数据
     * @param name 字段序号vno
     * @param t 当字段不存在时是否抛出异常
     * @return 字段元数据对象，若不存在则返回null
     */
    public XsFieldMeta getField(int name, boolean t) {
        if (!this.vnoMap.containsKey(name)) {
            if (t) throw new XsException("No exists field with vno: `" + name + "`");
            return null;
        }
        return getField(this.vnoMap.get(name), t);
    }

    public XsFieldMeta getField(int name) {
        return getField(name, true);
    }

    /**
     * 获取项目字段元数据
     * @param name 字段名称
     * @param t 当字段不存在时是否抛出异常
     * @return 字段元数据对象，若不存在则返回null
     */
    public XsFieldMeta getField(String name, boolean t) {
        if (!this.fields.containsKey(name)) {
            if (t) throw new XsException("No exists field with name: `" + name + "`");
            return null;
        }
        return this.fields.get(name);
    }

    public XsFieldMeta getField(String name) {
        return getField(name, true);
    }

    public boolean checkValid() {
        return checkValid(false);
    }

    /**
     * 判断该字段方案是否有效、可用
     * 每个方案必须并且只能包含一个类型为ID的字段
     * @param th 当没有通过检测时是否抛出异常，默认为false
     * @return 有效返回true，无效返回false
     */
    public boolean checkValid(boolean th) {
        if (!this.typeMap.containsKey(XsFieldMeta.TYPE.ID)) {
            if (th) {
                throw new RuntimeException("Missing field of type ID");
            }
            return false;
        }
        return true;
    }

    /**
     * 获取项目所有字段结构设置
     * @return
     */
    public XsFieldMeta[] getAllFields() {
        return this.fields.values().toArray(new XsFieldMeta[0]);
    }

    public Map<Integer, String> getVnoMap() {
        return this.vnoMap;
    }

    /**
     * 获取主键字段元数据
     * @return 类型为ID的字段
     */
    public XsFieldMeta getFieldId()
    {
        if (this.typeMap.containsKey(XsFieldMeta.TYPE.ID)) {
            String name = this.typeMap.get(XsFieldMeta.TYPE.ID);
            return this.fields.get(name);
        }
        return null;
    }
}
