package com.hangjiayun.infrastructure;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XsIndex extends XsServer {
    private int bufSize = 0;
    private boolean rebuild = false;
    private ArrayList<XsCommand> buf = new ArrayList();
    private static ArrayList<XsServer> adds = new ArrayList<>();

    public XsIndex(String conn, Xs xs) {
        super(conn, xs);
    }

    public XsIndex(String conn) {
        this(conn, null);
    }

    public XsIndex(Xs xs) {
        this(null, xs);
    }

    /**
     * 增加一个同步索引服务器
     * @param conn 索引服务端连接参数
     * @return
     */
    public XsServer addServer(String conn) {
        XsServer srv = new XsServer(conn, this.xs);
        adds.add(srv);
        return srv;
    }

    /**
     * 执行服务器端指令并获取返回值
     * 重写此方法是为了同步到额外增加的多个索引服务器
     * @param cmd
     * @param resArg
     * @param resCmd
     * @return
     */
    public XsCommand execCommand(XsCommand cmd, int resArg, int resCmd) {
        XsCommand res = super.execCommand(cmd, resArg, resCmd);
        for (XsServer srv : adds) {
            srv.execCommand(cmd, resArg, resCmd);
        }
        return res;
    }

    public XsCommand execCommand(XsCommand cmd, int resArg) {
        return this.execCommand(cmd, resArg, XsCmd.XS_CMD_OK);
    }

    public XsCommand execCommand(XsCommand cmd) {
        return this.execCommand(cmd, XsCmd.XS_CMD_NONE, XsCmd.XS_CMD_OK);
    }

    /**
     * 强制刷新服务端的当前库的索引缓存
     * @return 刷新成功返回true，失败则返回false
     */
    public boolean flushIndex()
    {
        try {
            this.execCommand(new XsCommand(XsCmd.XS_CMD_INDEX_COMMIT, XsCmd.XS_CMD_OK_DB_COMMITED));
        } catch (XsException e) {
            if (e.getCode() == XsCmd.XS_CMD_ERR_BUSY || e.getCode() == XsCmd.XS_CMD_ERR_RUNNING) {
                return false;
            }
            throw e;
        }
        return true;
    }

    /**
     * 开启索引命令提交缓冲区
     * 为优化网络性能，有必要先将本地提交的 add/update/del 等索引变动指令缓存下来
     * 当总大小达到参数指定的 size 时或调用{@link XsIndex#closeBuffer}时再真正提交到服务器
     * 注意：此举常用于需要大批量更新索引时，此外重复调用本函数是无必要的
     * @param size 缓冲区大小，单位MB，默认为4MB
     * @return 返回自身对象以支持串接操作
     */
    public XsIndex openBuffer(int size) {
        if (!this.buf.isEmpty()) {
            this.addExdata(this.buf, false);
        }
        this.bufSize = size << 20;
        this.buf.clear();
        return this;
    }

    public XsIndex openBuffer() {
        return this.openBuffer(4);
    }

    /**
     * 提交所有指令并关闭缓冲区
     * 若未曾打开缓冲区，调用本方法是无意义的
     * @return 返回自身对象以支持串接操作
     */
    public XsIndex closeBuffer() {
        return this.openBuffer(0);
    }

    /**
     * 完全清空索引数据
     * 如果当前数据库处于重建过程中将禁止清空
     * @return 返回自身对象以支持串接操作
     * @see #beginRebuild()
     */
    public XsIndex clean() {
        this.execCommand(new XsCommand(XsCmd.XS_CMD_INDEX_CLEAN_DB, XsCmd.XS_CMD_OK_DB_CLEAN));
        return this;
    }

    /**
     * 开始重建索引
     * 此后所有的索引更新指令将写到临时库，而不是当前搜索库，重建完成后调用{@link XsIndex#endRebuild}实现平滑重建索引，重建过程仍可搜索旧的索引库
     * 如直接用{@link XsIndex#clean}清空数据，则会导致重建过程搜索到不全的数据
     * @return 返回自身对象以支持串接操作
     */
    public XsIndex beginRebuild() {
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_INDEX_REBUILD, 0);
        this.execCommand(cmd, XsCmd.XS_CMD_OK_DB_REBUILD);
        this.rebuild = true;
        return this;
    }

    /**
     * 完成并关闭重建索引
     * 重建完成后调用，用重建好的索引数据代替旧的索引数据
     * @return 返回自身对象以支持串接操作
     */
    public XsIndex endRebuild() {
        if (this.rebuild) {
            this.rebuild = false;
            this.execCommand(new XsCommand(XsCmd.XS_CMD_INDEX_REBUILD, 1), XsCmd.XS_CMD_OK_DB_REBUILD);
        }
        return this;
    }

    /**
     * 中止重建索引
     * 丢弃重建临时库的所有数据，恢复成当前搜索库，主要用于偶尔重建意外中止的情况
     * @return 返回自身对象以支持串接操作
     * @see XsIndex#beginRebuild()
     */
    public XsIndex stopRebuild() {
        try {
            this.execCommand(new XsCommand(XsCmd.XS_CMD_INDEX_REBUILD, 2), XsCmd.XS_CMD_OK_DB_REBUILD);
            this.rebuild = false;
        } catch (XsException e) {
            if (e.getCode() != XsCmd.XS_CMD_ERR_WRONGPLACE) {
                throw e;
            }
        }
        return this;
    }

    /**
     * 更改存放索引数据的目录
     * 默认索引数据保存到服务器的db目录，通过此方法修改数据目录名
     * @param name 数据库名称
     * @return 返回自身对象以支持串接操作
     */
    public XsIndex setDb(String name) {
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_INDEX_SET_DB);
        cmd.buf = ByteBuffer.wrap(name.getBytes(StandardCharsets.UTF_8));
        this.execCommand(cmd, XsCmd.XS_CMD_OK_DB_CHANGED);
        return this;
    }

    public XsIndex add(XsDocument doc) {
        return this.update(doc, true);
    }

    public XsIndex update(XsDocument doc) {
        return this.update(doc, false);
    }

    public XsIndex update(XsDocument doc, boolean add) {
        //before submit
        if (!doc.beforeSubmit(this)) {
            return this;
        }

        XsFieldMeta fid = this.xs.getFieldId();
        String key = doc.f(fid.name);
        if (key == null || key.isEmpty()) {
            throw new XsException("Missing value of primary key (FIELD: "+fid+")");
        }

        //request cmd
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_INDEX_REQUEST, XsCmd.XS_CMD_INDEX_REQUEST_ADD);
        if (!add) {
            cmd.arg1 = XsCmd.XS_CMD_INDEX_REQUEST_UPDATE;
            cmd.arg2 = fid.vno;
            cmd.buf = ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8));
        }
        ArrayList<XsCommand> cmds = new ArrayList<>();
        cmds.add(cmd);

        //document cmds
        for (XsFieldMeta field : this.xs.getAllFields()) {
            String value = doc.f(field.name);
            if (value != null) {
                int varg = field.isNumeric() ? XsCmd.XS_CMD_VALUE_FLAG_NUMERIC : 0;
                value = field.val(value);
                if (!field.hasCustomTokenizer()) {
                    //internal tokenizer
                    int wdf = field.weight | (field.withPos() ? XsCmd.XS_CMD_INDEX_FLAG_WITHPOS : 0);
                    if (field.hasIndexMixed()) {
                        cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_INDEX, wdf, XsFieldSchema.MIXED_VNO, ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8))));
                    }
                    if (field.hasIndexSelf()) {
                        wdf |= field.isNumeric() ? 0 : XsCmd.XS_CMD_INDEX_FLAG_SAVEVALUE;
                        cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_INDEX, wdf, field.vno, ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8))));
                    }
                    //add value
                    if (!field.hasIndexSelf() || field.isNumeric()) {
                        cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_VALUE, varg, field.vno, ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8))));
                    }
                } else {
                    // add index
                    if (field.hasIndex()) {
                         String[] terms = field.getCustomTokenizer().getTokens(value, doc);
                         //self: [bool term, NOT weight, NOT stem, NOT pos]
                        if (field.hasIndexSelf()) {
                            int wdf = field.isBoolIndex() ? 1 : (field.weight | XsCmd.XS_CMD_INDEX_FLAG_CHECKSTEM);
                            for (String term : terms) {
                                //注意要计算其utf8编码下占用的字节数
                                if (term.getBytes(StandardCharsets.UTF_8).length > 200) {
                                    continue;
                                }
                                term = term.toLowerCase();
                                cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_TERM, wdf, field.vno, ByteBuffer.wrap(term.getBytes(StandardCharsets.UTF_8))));
                            }
                        }
                        // mixed: [use default tokenizer]
                        if (field.hasIndexMixed()) {
                            String mtext = String.join(" ", terms);
                            cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_INDEX, field.weight, XsFieldSchema.MIXED_VNO, ByteBuffer.wrap(mtext.getBytes(StandardCharsets.UTF_8))));
                        }
                    }
                    // add value
                    cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_VALUE, varg, field.vno, ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8))));
                }
            }
            // process add terms
            Map<String, Integer> terms = doc.getAddTerms(field);
            if (terms != null) {
                int wdf1 = field.isBoolIndex() ? 0 : XsCmd.XS_CMD_INDEX_FLAG_CHECKSTEM;
                for (Map.Entry<String, Integer> entry : terms.entrySet()) {
                    String term = entry.getKey().toLowerCase();
                    byte[] termBytes = term.getBytes(StandardCharsets.UTF_8);
                    if (termBytes.length > 200) {
                        continue;
                    }
                    int wdf2 = field.isBoolIndex() ? 1 : entry.getValue() * field.weight;
                    while (wdf2 > XsFieldMeta.MAX_WDF) {
                        cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_TERM, wdf1|XsFieldMeta.MAX_WDF, field.vno, ByteBuffer.wrap(termBytes)));
                        wdf2 -= XsFieldMeta.MAX_WDF;
                    }
                    cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_TERM, wdf1|wdf2, field.vno, ByteBuffer.wrap(termBytes)));
                }
            }
            // process add text
            String text = doc.getAddIndex(field);
            if (text != null) {
                byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);
                if (!field.hasCustomTokenizer()) {
                    int wdf = field.weight | (field.withPos() ? XsCmd.XS_CMD_INDEX_FLAG_WITHPOS : 0);
                    cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_INDEX, wdf,field.vno, ByteBuffer.wrap(textBytes)));
                } else {
                    // NOT pos
                    int wdf = field.isBoolIndex() ? 1 : (field.weight | XsCmd.XS_CMD_INDEX_FLAG_CHECKSTEM);
                    String[] xterms = field.getCustomTokenizer().getTokens(text, doc);
                    for (String term : xterms) {
                        byte[] termBytes = term.toLowerCase().getBytes(StandardCharsets.UTF_8);
                        if (termBytes.length > 200) {
                            continue;
                        }
                        cmds.add(new XsCommand(XsCmd.XS_CMD_DOC_TERM, wdf, field.vno, ByteBuffer.wrap(termBytes)));
                    }
                }
            }
        }

        //submit cmd
        cmds.add(new XsCommand(XsCmd.XS_CMD_INDEX_SUBMIT));

        //execute cmd
        if (this.bufSize > 0) {
            this.appendBuffer(cmds);
        } else {
            int i = 0;
            for (; i < cmds.size()-1; i++) {
                this.execCommand(cmds.get(i));
            }
            this.execCommand(cmds.get(i), XsCmd.XS_CMD_OK_RQST_FINISHED);
        }

        //after submit
        doc.afterSubmit(this);
        return this;
    }

    /**
     * 追加缓冲区命令数据
     * 若增加后的数据长度达到缓冲区最大值则触发一次服务器提交
     * @param buf 命令封包数组
     */
    private void appendBuffer(ArrayList<XsCommand> buf) {
        this.buf.addAll(buf);
        int bufSize = 0;
        for (XsCommand xsCommand : this.buf) {
            bufSize += xsCommand.toBytes().capacity();
        }
        if (bufSize > this.bufSize) {
            this.addExdata(this.buf, false);
            this.buf.clear();
        }
    }

    /**
     * 批量提交索引命令封包数据
     * 把多个命令封包内容连续保存为文件或变量，然后一次性提交以减少网络开销提升性能
     * @param data 要提交的命令封包数据，或存储命令封包的文件路径，编码必须已经是UTF-8
     * @param checkFile 是否检测参数为文件的情况
     * @return 返回自身对象以支持串接操作
     */
    public XsIndex addExdata(ArrayList<XsCommand> data, boolean checkFile) {
        if (checkFile) {
            //todo: 暂未实现该部分逻辑
            throw new XsException("Failed to read exdata from file");
        }

        // try to check allowed (BUG: check the first cmd only):
        // XS_CMD_IMPORT_HEADER, XS_CMD_INDEX_REQUEST, XS_CMD_INDEX_REMOVE, XS_CMD_INDEX_EXDATA
        XsCommand first = data.get(0);
        if (first.cmd != XsCmd.XS_CMD_IMPORT_HEADER
                && first.cmd != XsCmd.XS_CMD_INDEX_REQUEST
                && first.cmd != XsCmd.XS_CMD_INDEX_SYNONYMS
                && first.cmd != XsCmd.XS_CMD_INDEX_REMOVE
                && first.cmd != XsCmd.XS_CMD_INDEX_EXDATA
        ) {
            throw new XsException("Invalid start command of exdata (CMD: "+first.cmd+")");
        }

        //create cmd & execute it
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_INDEX_EXDATA);
        int totalCapacity = 0;
        for(XsCommand command : data) {
            totalCapacity += command.toBytes().capacity();
        }
        cmd.buf = ByteBuffer.allocate(totalCapacity);
        for (XsCommand command : data) {
            cmd.buf.put(command.toBytes());
        }
        this.execCommand(cmd, XsCmd.XS_CMD_OK_RQST_FINISHED);
        return this;
    }

    public XsIndex addExdata(ArrayList<XsCommand> data) {
        return this.addExdata(data, false);
    }
}
