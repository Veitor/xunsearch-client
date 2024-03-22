package com.hangjiayun.infrastructure;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class XsFieldMeta {
    public static final int MAX_WDF = 0x3f;
    enum TYPE {
        STRING(0),
        NUMERIC(1),
        DATE(2),
        ID(10),
        TITLE(11),
        BODY(12);

        public final int value;

        TYPE(int value) {
            this.value = value;
        }
    }

    enum FLAG {
        INDEX_SELF(0x01),
        INDEX_MIXED(0x02),
        INDEX_BOTH(0x03),
        WITH_POSITION(0x10),
        NON_BOOL(0x80);
        private final int value;
        FLAG(int value) {
            this.value = value;
        }
    }
    /**
     * 索引标志常量定义
     */
    public static final int FLAG_WITH_POSITION = 0x10;
    public static final int FLAG_NON_BOOL = 0x80;// 强制让该字段参与权重计算 (非布尔)
    /**
     * 字段名称
     * 理论上支持各种可视字符, 推荐字符范围:[0-9A-Za-z-_], 长度控制在 1~32 字节为宜
     */
    public String name;
    /**
     * 剪取长度（单位：字节）
     * 用于在返回搜索结果自动剪取较长内容的字段, 默认为 0表示不截取, body 型字段默认为 300 字节
     */
    public int cutlen = 0;
    /**
     * 混合区检索时的相对权重
     * 取值范围: 1~63, title 类型的字段默认为 5, 其它字段默认为 1
     */
    public int weight = 1;
    /**
     * 字段类型
     */
    public TYPE type = TYPE.STRING;
    /**
     * 字段序号
     * 取值为 0~255, 同一字段方案内不能重复, 由 {@link XsFieldSchema::addField()} 进行确定
     */
    public int vno = 0;
    /**
     * 词法分析器
     */
    private String tokenizer = XsTokenizer.DFL;
    /**
     * 索引标志设置
     */
    private int flag = 0;
    /**
     * 分词器实例缓存
     */
    private static Map<String, XsTokenizer> tokenizers = new HashMap<>();
    public XsFieldMeta(String name, Map<String, String> config) {
        this.name = name;
        this.fromConfig(config);
    }

    public void fromConfig(Map<String, String> config) {
        if (config.containsKey("type")) {
            String type = config.get("type").toUpperCase();
            try {
                this.type = TYPE.valueOf(TYPE.class, type);
                if (this.type == TYPE.ID) {
                    this.flag = FLAG.INDEX_SELF.value;
                    this.tokenizer = "full";
                } else if (this.type == TYPE.TITLE) {
                    this.flag = FLAG.INDEX_BOTH.value | FLAG.WITH_POSITION.value;
                    this.weight = 5;
                } else if (this.type == TYPE.BODY) {
                    this.vno = XsFieldSchema.MIXED_VNO;
                    this.flag = FLAG.INDEX_SELF.value | FLAG.WITH_POSITION.value;
                    this.cutlen = 300;
                }
            } catch (Exception e) {
            }
        }
        // index flag
        if (config.containsKey("index") && this.type != TYPE.BODY) {
            try {
                int localFlag = FLAG.valueOf(FLAG.class,"INDEX_"+config.get("index").toUpperCase()).value;
                this.flag &= ~ FLAG.INDEX_BOTH.value;
                this.flag |= localFlag;
            } catch (Exception e) {
            }
            if (this.type == TYPE.ID) {
                this.flag |= FLAG.INDEX_SELF.value;
            }

        }
        //others
        if (config.containsKey("cutlen")) {
            this.cutlen = Integer.parseInt(config.get("cutlen"));
        }
        if (config.containsKey("weight") && this.type != TYPE.BODY) {
            this.weight = Integer.parseInt(config.get("weight")) & MAX_WDF;
        }
        if (config.containsKey("phrase")) {
            if (config.get("phrase").compareToIgnoreCase("yes") == 0) {
                this.flag |= FLAG.NON_BOOL.value;
            } else if (config.get("phrase").compareToIgnoreCase("no") == 0) {
                this.flag &= ~ FLAG.NON_BOOL.value;
            }
        }
        if (config.containsKey("tokenizer") && this.type != TYPE.ID && !config.get("tokenizer").equals("default")) {
            this.tokenizer = config.get("tokenizer");
        }
    }

    public boolean isSpecial() {
        return (this.type == TYPE.ID || this.type == TYPE.TITLE || this.type == TYPE.BODY);
    }

    /**
     * 判断当前字段是否采用自定义分词器
     * @return 是返回 true，不是返回 false
     */
    public boolean hasCustomTokenizer() {
        return !XsTokenizer.DFL.equals(this.tokenizer);
    }

    /**
     * 获取自定义分词法分词器
     * @return 获取当前字段的自定义词法分析器
     */
    public XsTokenizer getCustomTokenizer() {
        if (tokenizers.containsKey(this.tokenizer)) {
            return tokenizers.get(this.tokenizer);
        } else {
            String tokenizer = this.tokenizer.trim();
            int leftPos = tokenizer.indexOf("(");
            int rightPos = tokenizer.indexOf(")");
            String name;
            String arg = null;
            if (leftPos>0 && rightPos>leftPos) {
                name = "XsTokenizer" + tokenizer.substring(0, 1).toUpperCase() + tokenizer.substring(1, leftPos);
                arg = tokenizer.substring(leftPos+1, rightPos);
            } else {
                name = "XsTokenizer" + tokenizer.substring(0, 1).toUpperCase() + tokenizer.substring(1);
                arg = null;
            }

            //todo: 目前这里仅支持加载内置分析器，后续考虑在这里加上可以加载用户自定义的分析器
            Class<XsTokenizer> clazz;
            try {
                String fullQualifiedName = this.getClass().getPackageName()+".tokenizer."+name;
                clazz = (Class<XsTokenizer>) Class.forName(fullQualifiedName);
            } catch (Exception e) {
                throw new XsException("Undefined custom tokenizer `"+tokenizer+"` for field `"+this.name+"`");
            }

            try {
                XsTokenizer obj = arg == null ? clazz.getDeclaredConstructor().newInstance() : clazz.getDeclaredConstructor(String.class).newInstance(arg);
                tokenizers.put(tokenizer, obj);
                return obj;
            } catch (Exception e) {
                throw new XsException("instance XsTokenizer failed", e);
            }
        }
    }

    /**
     * 判断当前字段的索引是否为布尔型
     * 目前只有内置分词器支持语法型索引，自1.0.1版本其把费索引字段也视为布尔便于判断
     * @return 是返回 true，不是返回 false
     */
    public boolean isBoolIndex() {
        if ((this.flag & FLAG_NON_BOOL) > 0) {
            return false;
        }
        return (!this.hasIndex() || !Objects.equals(this.tokenizer, XsTokenizer.DFL));
    }

    /**
     * 判断当前字段是否需要索引
     * @return
     */
    public boolean hasIndex() {
        return (this.flag & FLAG.INDEX_BOTH.value) > 0;
    }

    /**
     * 判断当前字段是否为数字型
     * @return
     */
    public boolean isNumeric() {
        return this.type == TYPE.NUMERIC;
    }

    /**
     * 把给定的值转换为符合这个字段的数据格式
     * 【注意！】该实现与PHP版本略有不同。主要是不支持PHP中的strtotime的实现，具体对照PHP版本了解详情
     * @param value 原值
     * @return 转换后的值
     */
    public String val(String value) {
        if (this.type == TYPE.DATE) {
            boolean isNumeric = false;
            Long timestamp = null;
            try {
                timestamp = Long.parseLong(value);
                isNumeric = true;
            } catch (Exception e) {
                isNumeric = false;
            }
            //注意，这里与php版本的实现有所不同，暂时不支持类似php中使用strtotime将自然语言转换为时间戳。这里只支持纯数字时间戳
            if (isNumeric && value.length() != 8) {
                Date date = new Date(timestamp);
                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd");
                return outputFormat.format(date);
            }
        }
        return value;
    }

    /**
     * 判断当前字段索引是否支持短语搜索
     * @return 是返回true，不是返回false
     */
    public boolean withPos() {
        return (this.flag & FLAG_WITH_POSITION) > 0;
    }

    /**
     * 判断当前字段是否需要在混合区索引
     * @return 若需要返回true，不需要则返回false
     */
    public boolean hasIndexMixed(){
        return (this.flag & FLAG.INDEX_MIXED.value) > 0;
    }

    /**
     * 判断当前字段是否需要在字段区索引
     * @return 若需要返回true，不需要则返回false
     */
    public boolean hasIndexSelf() {
        return (this.flag & FLAG.INDEX_SELF.value) > 0;
    }


}
