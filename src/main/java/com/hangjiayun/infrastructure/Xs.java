package com.hangjiayun.infrastructure;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Xs extends XsComponent{
    /**
     * 索引操作对象
     */
    private XsIndex xsIndex;
    /**
     * 搜索操作对象
     */
    private XsSearch xsSearch;
    /**
     * scws分词服务器
     */
    private XsServer xsServer;
    /**
     * 当前字段方案
     */
    private XsFieldSchema schema;
    private XsFieldSchema bindSchema;
    /**
     * 最近创建的Xs对象
     */
    private static Xs lastXs;
    /**
     * 对ini配置内容解析后得到的数据配置
     * 第一层map的key是字段名，value是第二层的字段的配置map
     * 第二层的字段配置map的key是配置名，value是配置值
     */
    private Map<String, Object> config;

    public Xs(String file) {
        this.loadIniFile(file);
        lastXs = this;
    }

    private void loadIniFile(String filePathOrContent) {
        boolean cache = false;
        File file = new File(filePathOrContent);
        String data;
        if (file.isFile()) {
            try(BufferedReader reader = new BufferedReader((new FileReader(file)))) {
                String line;
                StringBuilder content = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    content.append(line).append("\n");
                }
                data = content.toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            data = filePathOrContent;
        }
        this.config = this.parseIniData(data);
        if (this.config.isEmpty()) {
            throw new XsException("Failed to parse project config file/string: " + filePathOrContent.substring(0, 15));
        }
        XsFieldSchema schema = new XsFieldSchema();
        for (Map.Entry<String, Object> entry : this.config.entrySet()) {
            if (entry.getValue() instanceof Map) {
                schema.addField(entry.getKey(), (Map<String, String>)entry.getValue());
            }
        }
        schema.checkValid(true);

        //暂时注释这个project.name配置
        if (!this.config.containsKey("project.name")) {
            int idx = filePathOrContent.lastIndexOf("/");
            String projectName = idx >=0 ? filePathOrContent.substring(idx+1) : filePathOrContent;
            this.config.put("project.name", projectName);
        }
        this.schema = this.bindSchema = schema;
        //todo: ...其他的缓存逻辑，暂时不加
    }

    /**
     * 解析INI配置文件
     * @param data ini配置文件内容
     * @return 解析后的数据格式
     */
    private Map<String, Object> parseIniData(String data) {
        //注意，这里使用LinkedHashMap来保证配置字段的顺序，迅搜实现逻辑里依赖配置里声明的字段的出现顺序
        Map<String, Object> ret = new LinkedHashMap<>();
        Map<String, Object> cur = ret;
        for (String line : data.split("\n")) {
            if (line.equals("") || line.charAt(0) == ';' || line.charAt(0) == '#') {
                continue;
            }
            // ^\\s+ 匹配开头的空格
            // \\s+$ 匹配结尾的空格
            line = line.replaceAll("^\\s+|\\s+$", "");//类似php中trim函数的效果
            if (line.equals("")) {
                continue;
            }
            if (line.charAt(0) == '[' && line.charAt(line.length() - 1) == ']') {
                String sec = line.substring(1, line.length()-1);
                cur = new HashMap<>();
                ret.put(sec, cur);
                continue;
            }
            int pos = line.indexOf("=");
            if (pos == -1) continue;
            String key = line.substring(0,pos).trim();
            String value = line.substring(pos+1).trim();
            cur.put(key, value);
        }
        return ret;
    }

    /**
     * 改变项目的默认字符集
     * @param charset 修改后的字符集
     */
    public void setDefaultCharset(String charset) {
        this.config.put("project.default_charset", charset.toUpperCase());
    }

    /**
     * 获取索引操作对象
     * @return 索引操作对象
     */
    public XsIndex getIndex() {
        if (this.xsIndex == null) {
            ArrayList<String> adds = new ArrayList<>();
            String conn = this.config.containsKey("server.index") ? (String) this.config.get("server.index") : "8383";
            String[] connArr = conn.split(";");
            if (connArr.length >= 1) {
                conn = connArr[0];
            }
            this.xsIndex = new XsIndex(conn, this);
            this.xsIndex.setTimeout(0);
            for (int i=1;i<connArr.length;i++) {
                conn = connArr[i].trim();
                if (!conn.isEmpty() && !conn.isBlank()) {
                    this.xsIndex.addServer(conn).setTimeout(0);
                }
            }
        }
        return this.xsIndex;
    }

    public XsSearch getSearch() {
        if (this.xsSearch == null) {
            ArrayList<String> conns = new ArrayList<>(10);
            if (!this.config.containsKey("server.search")) {
                conns.add("8384");
            } else {
                for (String conn : ((String)this.config.get("server.search")).split(";")) {
                    if (!conn.isBlank()) {
                        conns.add(conn);
                    }
                }
            }
            if (conns.size() > 1) {
                Collections.shuffle(conns);
            }
            for (int i = 0; i<conns.size(); i++) {
                try {
                    this.xsSearch = new XsSearch(conns.get(i), this);
                    this.xsSearch.setCharset(this.getDefaultCharset());
                } catch (XsException e) {
                    if (conns.size() == (i+1)) {
                        throw e;
                    }
                }
            }
        }
        return this.xsSearch;
    }

    public String getName() {
        return (String)this.config.get("project.name");
    }

    /**
     * 获取项目的默认字符集
     * @return
     */
    public String getDefaultCharset() {
        return this.config.containsKey("project.default_charset") ? ((String)this.config.get("project.default_charset")).toUpperCase() : "UTF-8";
    }

    public XsFieldMeta getField(String name, boolean t) {
        return this.schema.getField(name, t);
    }

    public XsFieldMeta getField(String name) {
        return getField(name, true);
    }

    /**
     * 字符编码转换
     * @param data 需要转换的数据
     * @param to 转换后的字符编码
     * @param from 转换前的字符编码
     * @return
     */
    public static String convert(String data, String to, String from) {
        if (to.equals(from)) {
            return data;
        }
        Pattern pattern = Pattern.compile("[\\x81-\\xFE]");
        Matcher matcher = pattern.matcher(data);
        if (matcher.find()) {
            byte[] fromByte = data.getBytes(Charset.forName(from));//将字符串以from指定的编码解析出字节数组
            data = new String(fromByte, Charset.forName(to));//将字节数组以to指定的编码进行解读作为外码
        }
        return data;
    }

    public XsFieldMeta[] getAllFields() {
        return this.schema.getAllFields();
    }

    /**
     * 获取当前在用的字段拿方案
     * 通用于搜索结果文档和修改、添加的索引文档
     * @return 当前字段方案
     */
    public XsFieldSchema getSchema() {
        return this.schema;
    }

    /**
     * 获取当前主键字段
     * @return 类型为ID的字段
     * @see XsFieldSchema::getField()
     */
    public XsFieldMeta getFieldId() {
        return this.schema.getFieldId();
    }
}
