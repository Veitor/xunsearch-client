package com.hangjiayun.infrastructure;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XsSearch extends XsServer{
    /**
     * 搜索结果默认分页数量
     */
    private static final int PAGE_SIZE = 10;
    private static final String LOG_DB = "log_db";
    private String charset = "UTF-8";
    private int defaultOp = XsCmd.XS_CMD_QUERY_OP_AND;
    private Map<String, Boolean> prefix;
    private boolean fieldSet = false;
    private Integer count;
    private Integer lastCount;
    private String query;
    private boolean resetScheme = false;
    private int limit = 0;
    private int offset = 0;
    private ArrayList<String> terms = new ArrayList<>();
    private String curDb;
    private String[] curDbs;
    private String highlight;
    private Map<String, Map<String, Integer>> facets = new HashMap<>();


    public XsSearch(String conn, Xs xs) {
        super(conn, xs);
    }

    public XsSearch(String conn) {
        this(conn, null);
    }

    public XsSearch(Xs xs) {
        this(null, xs);
    }

    /**
     * 连接搜索服务端并初始化
     * 每次重新连接后所有的搜索语句相关设置均被还原
     * @param conn
     */
    @Override
    public void open(String conn) {
        super.open(conn);
        this.prefix = new HashMap<>();
        this.fieldSet = false;
        this.lastCount = 0;
    }

    /**
     * 设置默认字符集
     * 默认字符集是UTF-8，如果您提交的搜索语句和预期得到的搜索结果为其他字符集，请先设置
     * @param charset
     * @return
     */
    public XsSearch setCharset(String charset) {
        this.charset = charset.toUpperCase();
        if (this.charset.equals("UTF8")) {
            this.charset = "UTF-8";
        }
        return this;
    }

    /**
     * 开启模糊搜索
     * 默认情况只返回包含所有搜索词的记录，通过本方法可以获得更多搜索结果
     * @param value
     * @return
     */
    public XsSearch setFuzzy(boolean value) {
        this.defaultOp = value ? XsCmd.XS_CMD_QUERY_OP_OR : XsCmd.XS_CMD_QUERY_OP_AND;
        return this;
    }

    public XsSearch setFuzzy() {
        return setFuzzy(true);
    }

    public XsSearch setQuery(String query) {
        this.clearQuery();
        if (query != null) {
            this.query = query;
            this.addQueryString(query);
        }
        return this;
    }

    /**
     * 设置地理位置距离排序方式
     * @param fields 在此定义地理位置信息原点坐标信息，至少包含2个值
     * @param reverse 是否由远及近排序，默认为由远及近排序
     * @param relevanceFirst 是否优先相关性排序，默认为否
     * @return
     */
    public XsSearch setGeodistSort(Map<String, Float> fields, boolean reverse, boolean relevanceFirst) {
        if (fields.size() < 2) {
            throw new XsException("Fields of `setGeodistSort` should be an array contain two or more elements");
        }
        ArrayList<Byte> buf = new ArrayList<>();
//        StringBuilder buf = new StringBuilder();
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            XsFieldMeta field = this.xs.getField(entry.getKey(), true);
            if (!field.isNumeric()) {
                throw new XsException("Type of GeoField `"+entry.getKey()+"` shoud be numeric");
            }
            int vno = field.vno;
            String vbuf = String.valueOf(entry.getValue());
            byte[] vbufBytes = vbuf.getBytes(StandardCharsets.ISO_8859_1);
            int vlen = vbufBytes.length;
            if (vlen > 255) {
                throw new XsException("Value of `" + entry.getKey() + "` too long");
            }
            buf.add((byte)vno);
            buf.add((byte)vlen);
            for (byte b : vbufBytes) {
                buf.add(b);
            }
        }
        int type = XsCmd.XS_CMD_SORT_TYPE_GEODIST;
        if (relevanceFirst) {
            type |= XsCmd.XS_CMD_SORT_FLAG_RELEVANCE;
        }
        if (!reverse) {
            type |= XsCmd.XS_CMD_SORT_FLAG_ASCENDING;
        }
        byte[] bufB = new byte[buf.size()];
        for (int i=0;i<buf.size();i++) {
            bufB[i] = buf.get(i);
        }
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_SET_SORT, type, 0, ByteBuffer.wrap(bufB));
        this.execCommand(cmd);
        return this;
    }

    public XsSearch setGeodistSort(Map<String, Float> fields, boolean reverse) {
        return setGeodistSort(fields, reverse, false);
    }
    public XsSearch setGeodistSort(Map<String, Float> fields) {
        return setGeodistSort(fields, false);
    }

    /**
     * 设置多字段组合排序方式
     * 当您需要根据多个字段的值按不同的方式综合排序时，请使用这项
     * @param fields 排序依据的字段map，以字段为key名，true/false为值表示正序或逆序
     * @param reverse 是否为倒序显示，默认为正向，此处和{@see setSort}略有不同
     * @param relevanceFirst 是否优先相关性排序，默认为否
     * @return
     */
    public XsSearch setMultiSort(LinkedHashMap<String, Boolean> fields, boolean reverse, boolean relevanceFirst) {
        ArrayList<Byte> buf = new ArrayList<>();
        int vno;
        boolean asc;
        for (Map.Entry<String ,Boolean> entry:fields.entrySet()) {
            vno = this.xs.getField(entry.getKey(), true).vno;
            asc = entry.getValue();
            if (vno != XsFieldSchema.MIXED_VNO) {
                buf.add((byte)vno);
                buf.add((byte)(asc?1:0));
            }
        }
        if (!buf.isEmpty()) {
            int type = XsCmd.XS_CMD_SORT_TYPE_MULTI;
            if (relevanceFirst) {
                type |= XsCmd.XS_CMD_SORT_FLAG_RELEVANCE;
            }
            if (!reverse) {
                type |= XsCmd.XS_CMD_SORT_FLAG_ASCENDING;
            }
            byte[] bufB = new byte[buf.size()];
            for (int i=0;i<buf.size();i++) {
                bufB[i] = buf.get(i);
            }
            XsCommand cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_SET_SORT, type, 0, ByteBuffer.wrap(bufB));
            this.execCommand(cmd);
        }
        return this;
    }

    public XsSearch setMultiSort(String fields, boolean reverse, boolean relevanceFirst) {
        return this.setSort(fields, !reverse, relevanceFirst);
    }

    public XsSearch setMultiSort(LinkedHashMap<String, Boolean> fields, boolean reverse) {
        return setMultiSort(fields, reverse, false);
    }

    public XsSearch setMultiSort(LinkedHashMap<String, Boolean> fields) {
        return setMultiSort(fields, false);
    }

    /**
     * 设置搜索结果的排序方式
     * 注意，每当调用{@link setDb}或{@link addDb}修改当前数据库时会重置排序设定
     * @param field 依据指定字段的值排序，设为null使用默认顺序
     * @param asc 是否为正序排列，即从小到大，从少到多，默认为反序
     * @param relevanceFirst 是否优先相关性排序，默认为否
     * @return
     */
    public XsSearch setSort(String field, boolean asc, boolean relevanceFirst) {
        XsCommand cmd;
        if (field == null) {
            cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_SET_SORT, XsCmd.XS_CMD_SORT_TYPE_RELEVANCE);
        } else {
            int type = XsCmd.XS_CMD_SORT_TYPE_VALUE;
            if (relevanceFirst) {
                type |= XsCmd.XS_CMD_SORT_FLAG_RELEVANCE;
            }
            if (asc) {
                type |= XsCmd.XS_CMD_SORT_FLAG_ASCENDING;
            }
            cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_SET_SORT, type, this.xs.getField(field, true).vno);
        }
        this.execCommand(cmd);
        return this;
    }

    public XsSearch setSort(String name, boolean asc) {
        return setSort(name, asc, false);
    }

    public XsSearch setSort(String name) {
        return setSort(name, false);
    }

    /**
     * 清空默认搜索语句
     */
    public void clearQuery() {
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_QUERY_INIT);
        if (this.resetScheme) {
            cmd.arg1 = 1;
            this.prefix.clear();
            this.fieldSet = false;
            this.resetScheme = false;
        }
        this.execCommand(cmd);
        this.query = null;
        this.count = null;
        this.terms = null;
    }

    /**
     * 增加默认搜索语句
     * @param query 搜索语句
     * @param addOp 与旧语句的结合操作符，如果无旧语句或为空则这此无意义
     * @param scale 权重计算缩放比例，默认为1表示不缩放，其它值范围0.xx~655.35
     * @return 修正后的搜索语句
     */
    public String addQueryString(String query, int addOp, int scale) {
        query = this.preQueryString(query);
        ByteBuffer bscale = null;
        if (scale>0 && scale != 1) {
            bscale = ByteBuffer.allocate(2);
            bscale.putShort((short)(scale*100 & 0xffff));
        } else {
            bscale = ByteBuffer.allocate(0);
        }
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_QUERY_PARSE, addOp, this.defaultOp, ByteBuffer.wrap(query.getBytes(StandardCharsets.UTF_8)), bscale);
        this.execCommand(cmd);
        return query;
    }

    public String addQueryString(String query, int addOp) {
        return addQueryString(query, addOp, 1);
    }

    public String addQueryString(String query) {
        return addQueryString(query, XsCmd.XS_CMD_QUERY_OP_AND, 1);
    }

    private String preQueryString(String query) {
        query = query.trim();
        if (this.resetScheme) {
            this.clearQuery();
        }

        //init special field here
        this.initSpecialField();

        StringBuilder newQuery = new StringBuilder();
        for (String part : query.split("[ \t\r\n]+")) {
            if (part.isEmpty()) continue;
            if (!newQuery.toString().isEmpty()) {
                newQuery.append(" ");
            }
            int colonPos = part.indexOf(":", 1);
            if (colonPos != -1) {
                int i = 0;
                for (i=0; i<colonPos; i++) {
                    //这里目前暂时可以使用charAt判断，因为chat基于第一版原始的unicode编码规约，即使用16bit存储，下面这几个字符都是包含在内的
                    if (part.charAt(i) != '+' && part.charAt(i) != '-' && part.charAt(i) != '~' && part.charAt(i) != '(') {
                        break;
                    }
                }
                String name = part.substring(i, colonPos);
                XsFieldMeta field = this.xs.getField(name, false);
                if (field != null && field.vno != XsFieldSchema.MIXED_VNO) {
                    this.regQueryPrefix(name);
                    //todo：自定义分词部分暂未实现。一些字符串截取可能因为编码问题会处理不正确
                    if (field.hasCustomTokenizer()) {
                        String prefix = i>0 ? part.substring(0, i) : "";
                        String suffix = "";

                        String value = part.substring(colonPos+1);
                        if (value.endsWith(")")) {
                            suffix = ")";
                            value = value.substring(0, value.length() - 1);
                        }
                        String[] tokens = field.getCustomTokenizer().getTokens(value);
                        String[] terms = new String[tokens.length];
                        int fori;
                        for (fori=0;i<tokens.length;i++) {
                            terms[i] = tokens[i].toLowerCase();
                        }
                        //去重
                        Set<String> set = new HashSet<>(Arrays.asList(terms));
                        terms = set.toArray(new String[0]);
                        newQuery.append(prefix).append(name).append(":").append(String.join(" "+name+":", terms)).append(suffix);
                    } else if (part.charAt(colonPos+1) != '(' && part.matches("[\\x81-\\xFE]")) {
                        newQuery.append(part.substring(0, colonPos+1))
                                .append("(")
                                .append(part.substring(colonPos+1))
                                .append(")");
                    } else {
                        newQuery.append(part);
                    }
                    continue;
                }
            }
            Pattern pattern = Pattern.compile("[\\u0081-\\u00FE]");
            Matcher matcher = pattern.matcher(part);
            if (part.length()>1 && (part.charAt(0) == '+' || part.charAt(0) == '-') && part.charAt(1) != '(' && matcher.find()) {
                newQuery.append(part.substring(0, 1))
                        .append("(")
                        .append(part.substring(1))
                        .append(")");
                continue;
            }
            newQuery.append(part);
        }
        return Xs.convert(newQuery.toString(), "UTF-8", this.charset);
    }

    /**
     * 登记搜索语句中的字段
     * @param name 字段名称
     */
    private void regQueryPrefix(String name) {
        XsFieldMeta field = this.xs.getField(name, false);
        if (!this.prefix.containsKey(name) && field != null && field.vno != XsFieldSchema.MIXED_VNO) {
            int type = field.isBoolIndex() ? XsCmd.XS_CMD_PREFIX_BOOLEAN : XsCmd.XS_CMD_PREFIX_NORMAL;
            XsCommand cmd = new XsCommand(XsCmd.XS_CMD_QUERY_PREFIX, type, field.vno, ByteBuffer.wrap(name.getBytes(StandardCharsets.UTF_8)));
            this.execCommand(cmd);
            this.prefix.put(name, true);
        }
    }

    /**
     * 设置字符型字段即裁剪长度
     */
    private void initSpecialField() {
        if (this.fieldSet) {
            return;
        }
        for (XsFieldMeta field : this.xs.getAllFields()) {
            if (field.cutlen !=0 ) {
                int len = Math.min(127, (int)Math.ceil((double) field.cutlen/10));
                XsCommand cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_SET_CUT, len, field.vno);
                this.execCommand(cmd);
            }
            if (field.isNumeric()) {
                XsCommand cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_SET_NUMERIC, 0, field.vno);
                this.execCommand(cmd);
            }
        }
        this.fieldSet = true;
    }

    /**
     * 添加过滤搜索区间或范围
     * @param field
     * @param from 起始值（不包含），若为null则相当于匹配<=to（字典顺序）
     * @param to 结束值（包含），若为null则相当于匹配>=from（字典顺序）
     * @return
     */
    public XsSearch addRange(String field, Integer from, Integer to) {
        if (from != null || to != null) {
            int vno = this.xs.getField(field).vno;
            XsCommand cmd;
            if (from == null) {
                cmd = new XsCommand(XsCmd.XS_CMD_QUERY_VALCMP, XsCmd.XS_CMD_QUERY_OP_FILTER, vno, ByteBuffer.wrap(String.valueOf(to).getBytes(StandardCharsets.UTF_8)), ByteBuffer.wrap(String.valueOf(XsCmd.XS_CMD_VALCMP_LE).getBytes(StandardCharsets.UTF_8)));
            } else if (to == null) {
                cmd = new XsCommand(XsCmd.XS_CMD_QUERY_VALCMP, XsCmd.XS_CMD_QUERY_OP_FILTER, vno, ByteBuffer.wrap(String.valueOf(from).getBytes(StandardCharsets.UTF_8)), ByteBuffer.wrap(String.valueOf(XsCmd.XS_CMD_VALCMP_GE).getBytes(StandardCharsets.UTF_8)));
            } else {
                cmd = new XsCommand(XsCmd.XS_CMD_QUERY_RANGE, XsCmd.XS_CMD_QUERY_OP_FILTER, vno, ByteBuffer.wrap(String.valueOf(from).getBytes(StandardCharsets.UTF_8)), ByteBuffer.wrap(String.valueOf(to).getBytes(StandardCharsets.UTF_8)));
            }
            this.execCommand(cmd);
        }
        return this;
    }

    /**
     * 添加权重索引词
     * 无论是否包含这种词都不影响搜索匹配，但会参与计算结果权重，使结果的相关度更高
     * @param field 索引词所属的字段
     * @param term 索引词
     * @param weight 权重计算缩放比例
     * @return
     */
    public XsSearch addWeight(String field, String term, int weight) {
        return this.addQueryTerm(field, term, XsCmd.XS_CMD_QUERY_OP_AND_MAYBE, weight);
    }

    public XsSearch addWeight(String field, String term) {
        return addWeight(field, term, 1);
    }

    /**
     * 增加默认搜索词汇
     * @param field 索引词所属的字段，若为混合区词汇可设为null或body型的字段名
     * @param term 索引词
     * @param addOp 与旧语句的结合操作符，如果无旧语句或空则这此无意义
     * @param scale 权重计算缩放比例，默认为1表示不缩放，其他值0.xx~655.35
     * @return
     */
    public XsSearch addQueryTerm(String field, String term, int addOp, int scale) {
        ByteBuffer bscale = null;
        if (scale>0&&scale!=1) {
            bscale = ByteBuffer.allocate(2);
            bscale.putShort((short)(scale*100 & 0xffff));
        } else {
            bscale = ByteBuffer.allocate(0);
        }
        int vno = field == null ? XsFieldSchema.MIXED_VNO : this.xs.getField(field, true).vno;
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_QUERY_TERM, addOp, vno, ByteBuffer.wrap(term.getBytes(StandardCharsets.UTF_8)), bscale);
        this.execCommand(cmd);
        return this;
    }

    public XsSearch addQueryTerm(String field, String[] term, int addOp, int scale) {
        ByteBuffer bscale = null;
        if (scale>0&&scale!=1) {
            bscale = ByteBuffer.allocate(2);
            bscale.putShort((short)(scale*100 & 0xffff));
        } else {
            bscale = ByteBuffer.allocate(0);
        }
        int vno = field == null ? XsFieldSchema.MIXED_VNO : this.xs.getField(field, true).vno;
        if (term.length == 0) {
            return this;
        } else if (term.length == 1) {
            return addQueryTerm(field, term[0], addOp, scale);
        } else {
            XsCommand cmd = new XsCommand(XsCmd.XS_CMD_QUERY_TERMS, addOp, vno, ByteBuffer.wrap(String.join("\t", term).getBytes(StandardCharsets.UTF_8)), bscale);
            this.execCommand(cmd);
            return this;
        }
    }

    public XsSearch addQueryTerm(String field, String term, int addOp) {
        return addQueryTerm(field, term, addOp, 1);
    }

    public XsSearch addQueryTerm(String field, String term) {
        return addQueryTerm(field, term, XsCmd.XS_CMD_QUERY_OP_AND);
    }

    public XsSearch addQueryTerm(String field, String[] term, int addOp) {
        return addQueryTerm(field, term, addOp, 1);
    }

    public XsSearch addQueryTerm(String field, String[] term) {
        return addQueryTerm(field, term, XsCmd.XS_CMD_QUERY_OP_AND);
    }

    /**
     * 设置分面搜索记数
     * 用于记录匹配搜索结果中按字段值分组的数量统计，每次调用{@link search}后会还原设置
     * 对于多次调用exact参数以最后一次为准，只支持字段值不超过255字节的情况
     * @param field 要进行分组统计的字段值， 最多同时支持8个
     * @param exact 是否要求绝对精确搜索，这会造成较大的系统开销
     * @return
     */
    public XsSearch setFacets(String[] field, boolean exact) {
        byte[] buf = new byte[field.length];
        int i = 0;
        for (String name:field) {
            XsFieldMeta ff = this.xs.getField(name);
            if (ff.type != XsFieldMeta.TYPE.STRING) {
                throw new XsException("Field `"+name+"` cann't be used for facets search, can only be string type");
            }
            buf[i++] = (byte)ff.vno;
        }
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_SET_FACETS, exact?1:0);
        cmd.buf = ByteBuffer.wrap(buf);
        this.execCommand(cmd);
        return this;
    }

    public XsSearch setFacets(String field, boolean exact) {
        return setFacets(new String[]{field}, exact);
    }

    public XsSearch setFacets(String[] field) {
        return setFacets(field, false);
    }

    public XsSearch setFacets(String field) {
        return setFacets(field, false);
    }

    /**
     * 读取最近一次分面搜索记数
     * 必须在某一次{@link search}之后调用本函数才有意义
     * @param field 读取分面记数的字段
     * @return 返回由值和计数组成的Map，key是数据值，value是统计数量，若该字段不存在分面统计数据则返回null
     */
    public Map<String, Integer> getFacets(String field) {
        return this.facets.get(field);
    }

    public Map<String, Map<String, Integer>> getFacets() {
        return this.facets;
    }

    /**
     * 设置当前搜索语句的分词复合等级
     * 复合等级是 scws 分词粒度控制的一个重要参数，是长词细分处理依据，默认为3，值范围0~15
     * 注意：这个设置仅只对本次搜索有效，仅对设置之后的{@link setQuery}起作用，由于query
     * 设计的方式问题，目前无法支持搜索语句单字切分，但您可以在模糊检索时设为0来关闭复合分词
     * @param level 要设置的分词复合等级
     * @return
     */
    public XsSearch setScwsMulti(int level) {
        if (level >= 0 && level < 16) {
            XsCommand cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_SCWS_SET, XsCmd.XS_CMD_SCWS_SET_MULTI, level);
            this.execCommand(cmd);
        }
        return this;
    }

    /**
     * 设置搜索结果的数量和偏移
     * 用于搜索结果分页，每次调用{@link search}后会还原这2个变量到初始值
     * @param limit 数量上限，若设为0，则启用默认值PAGE_SIZE
     * @param offset 偏移量，即跳过的结果数量，默认为0
     * @return
     */
    public XsSearch setLimit(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
        return this;
    }

    public XsSearch setLimit(int limit) {
        return setLimit(limit, 0);
    }

    /**
     * 估算搜索语句的匹配数据量
     * @param query 搜索语句，若传入null使用默认语句，调用后会还原默认排序方式
     *              如果搜索语句和最近一次{@link search}的语句一样，请改用{@link getLastCount}以提升效率
     *              最大长度为80字节
     * @return 匹配的搜索结果数量，估算数值
     */
    public int count(String query) {
        query = query == null ? "" : this.preQueryString(query);
        if (query.isBlank() && this.count != null) {
            return this.count;
        }
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_GET_TOTAL, 0, this.defaultOp, ByteBuffer.wrap(query.getBytes(StandardCharsets.UTF_8)));
        XsCommand res = this.execCommand(cmd);
//        ByteBuffer byteBuffer = ByteBuffer.wrap(res.buf.getBytes(StandardCharsets.ISO_8859_1), 0, 4);
        int count = res.buf.order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (query.isBlank()) {
            this.count = count;
        }
        return count;
    }

    public int count() {
        return count(null);
    }

    public List<XsDocument> search(String query, boolean saveHighlight) {
        if (!LOG_DB.equals(this.curDb) && saveHighlight) {
            this.highlight = query;
        }
        query = query == null ? "" : this.preQueryString(query);
        ByteBuffer page = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(this.offset);
        page.putInt(this.limit>0?this.limit:PAGE_SIZE);
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_SEARCH_GET_RESULT, 0, this.defaultOp, ByteBuffer.wrap(query.getBytes(StandardCharsets.UTF_8)), page);
        XsCommand res = this.execCommand(cmd, XsCmd.XS_CMD_OK_RESULT_BEGIN);
        this.lastCount = res.buf.order(ByteOrder.LITTLE_ENDIAN).getInt();

        // load vno map to name of fields
        List<XsDocument> ret = new ArrayList<>(this.lastCount);
        Map<Integer, String> vnoes = this.xs.getSchema().getVnoMap();

        XsDocument doc = null;
        // get result documents;
        while (true) {
            res = this.getRespond();
            if (res.cmd == XsCmd.XS_CMD_SEARCH_RESULT_FACETS) {
                int off = 0;
                ByteBuffer byteBuffer = res.buf.order(ByteOrder.LITTLE_ENDIAN);
                while ((off+6) < res.buf.capacity()) {
                    int vno = byteBuffer.get();//偏移1字节
                    int vlen = byteBuffer.get();//偏移1字节
                    if (vnoes.containsKey(vno)) {
                        String name = vnoes.get(vno);
                        int num = byteBuffer.getInt();//偏移4字节【这里可能后续要注意一下int类型大小溢出】
                        byte[] value = new byte[vlen];
                        byteBuffer.get(value, 0, vlen);//该偏移是基于漆面偏移6字节后的偏移vlen字节
                        String valueStr = new String(value, StandardCharsets.UTF_8);
                        if (!this.facets.containsKey(name)) {
                            this.facets.put(name, new HashMap<>(){{
                                put(valueStr, num);
                            }});
                        } else {
                            this.facets.get(name).put(valueStr, num);
                        }
                    }
                    off += vlen + 6;
                }
            } else if (res.cmd == XsCmd.XS_CMD_SEARCH_RESULT_DOC) {
                // got new doc
                doc = new XsDocument(new String(res.buf.array(), StandardCharsets.ISO_8859_1), this.charset);
                ret.add(doc);
            } else if (res.cmd == XsCmd.XS_CMD_SEARCH_RESULT_FIELD) {
                if (doc != null) {
                    String name = vnoes.containsKey(res.getArg()) ? vnoes.get(res.getArg()) : String.valueOf(res.getArg());
                    doc.setField(name, new String(res.buf.array(), StandardCharsets.UTF_8));
                }
            } else if (res.cmd == XsCmd.XS_CMD_SEARCH_RESULT_MATCHED) {
                if (doc != null) {
                    doc.setField("matched", new ArrayList<>(Arrays.asList((new String(res.buf.array(), StandardCharsets.UTF_8). split(" ")))), true);
                }
            } else if (res.cmd == XsCmd.XS_CMD_OK && res.getArg() == XsCmd.XS_CMD_OK_RESULT_END) {
                break;
            } else {
                throw new XsException("Unexpected respond in search {CMD: "+res.cmd+", ARG: "+res.getArg()+"}");
            }
        }

        if (query.isEmpty()) {
            this.count = this.lastCount;
            if (!LOG_DB.equals(this.curDb)) {
                this.logQuery();
                if (saveHighlight) {
                    this.initHighlight();
                }
            }
        }

        this.limit = 0;
        this.offset = 0;
        return ret;
    }

    public List<XsDocument> search(String query) {
        return search(query, true);
    }

    public List<XsDocument> search() {
        return search(null);
    }



    private void logQuery(String query) {
        //todo: 该实现暂未完成
    }

    private void logQuery() {
        logQuery(null);
    }

    private void initHighlight() {
        //todo: 该实现暂未完成
    }

    /**
     * 获取最近那次搜索的匹配总数估值
     * @return 匹配数量，如果从未搜索则返回null
     */
    public int getLastCount() {
        return this.lastCount;
    }


}
