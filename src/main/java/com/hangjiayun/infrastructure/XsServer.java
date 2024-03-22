package com.hangjiayun.infrastructure;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;

public class XsServer extends XsComponent implements AutoCloseable{
    /**
     * 连接标志定义（常量）
     */
    private static final int FILE = 0x01;
    private static final int BROKEN = 0x02;

    /**
     * 服务端关联的XS对象
     */
    public Xs xs;

    /**
     * 连接信息字符串
     */
    protected String conn;
    protected int flag;
    protected StringBuilder sendBuffer = new StringBuilder();
    protected String project;

    /**
     * 代替php版本里的sock资源
     */
    protected Channel channel;

    public XsServer(String conn, Xs xs) {
        this.xs = xs;
        if (conn != null) {
            this.open(conn);
        }
    }

    public XsServer(String conn) {
        this(conn, null);
    }

    public XsServer(Xs xs) {
        this(null ,xs);
    }

    public void open(String conn) {
        this.close();
        this.conn = conn;
        this.flag = BROKEN;
        this.sendBuffer.setLength(0);
        this.project = null;
        this.connect();
        this.flag ^= BROKEN;
        if (this.xs instanceof Xs) {
            this.setProject(this.xs.getName());
        }
    }

    public void connect() {
        String conn = this.conn;
        String host = "";
        int port = -1;
        if (conn.matches("^\\d+$")) {
            host = "localhost";
            port = Integer.parseInt(conn);
        } else if (conn.startsWith("file://")) {
            try {
                this.channel = FileChannel.open(Path.of(new URI(conn)));
                return;
            } catch (Exception e) {
                throw new XsException("Failed to open local file for writing: `" + conn + "`");
            }
        } else if (conn.contains(":")) {
            String[] connArr = conn.split(":");
            if (connArr.length >= 2) {
                host = connArr[0];
                port = Integer.parseInt(connArr[1]);
            }
        } else {
            host = "unix://" + conn;
            port = -1;
        }
        try {
            SocketChannel channel = SocketChannel.open(new InetSocketAddress(host, port));
            channel.configureBlocking(true);
            channel.socket().setSoTimeout(30);
            this.channel = channel;
        } catch (Exception e) {
            StringBuilder err = new StringBuilder(e.getMessage());
            err.append("(C#").append(host).append(":").append(port).append(")");
            throw new RuntimeException(err.toString(), e);
        }
    }

    public void close(boolean ioError) {
        if (this.channel != null && (this.flag & BROKEN) <= 0) {
            if (!ioError && this.sendBuffer.length() != 0) {
                this.write(this.sendBuffer.toString());
                this.sendBuffer.setLength(0);
            }
            if (!ioError && (this.flag & FILE) <= 0) {
                if (this.channel instanceof WritableByteChannel) {
                    WritableByteChannel channel = (WritableByteChannel) this.channel;
                    ByteBuffer byteBuffer = ByteBuffer.wrap((new XsCommand(XsCmd.XS_CMD_QUIT).toString().getBytes()));
                    try {
                        channel.write(byteBuffer);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            try {
                this.channel.close();
            } catch (Exception e) {

            }
            this.flag |= BROKEN;
        }
    }

    @Override
    public void close() {
        this.close(false);
    }

    public void setProject(String name, String home) {
        if (!name.equals(this.project)) {
            this.execCommand(Map.of("cmd", XsCmd.XS_CMD_USE, "buf", ByteBuffer.wrap(name.getBytes(StandardCharsets.UTF_8)), "buf1", ByteBuffer.wrap(home.getBytes(StandardCharsets.UTF_8))), XsCmd.XS_CMD_OK_PROJECT);
            this.project = name;
        }
    }

    public void setProject(String name) {
        setProject(name, "");
    }

    /**
     * 设置服务端超时秒数
     * @param sec 秒数，设为0则永不超时直到客户端主动关闭
     */
    public void setTimeout(int sec) {
        XsCommand cmd = new XsCommand(XsCmd.XS_CMD_TIMEOUT);
        cmd.setArg(sec);
        this.execCommand(cmd, XsCmd.XS_CMD_OK_TIMEOUT_SET);
    }

    public XsCommand execCommand(XsCommand cmd, int resArg, int resCmd) {
        if ((cmd.cmd & 0x80) > 0) {
            this.sendBuffer.append(cmd.toString());
            return null;
        }
        String buf = this.sendBuffer.append(cmd.toString()).toString();
        this.sendBuffer.setLength(0);
        this.write(buf);
//        this.write(cmd.getBytes());

        if ((this.flag & FILE) > 0) {
            return null;
        }

        XsCommand res = this.getRespond();
        if (res.cmd == XsCmd.XS_CMD_ERR && resCmd != XsCmd.XS_CMD_ERR) {
            throw new XsException(res.buf.toString(), res.getArg());
        }
        if (res.cmd != resCmd || (resArg != XsCmd.XS_CMD_NONE && res.getArg() != resArg)) {
            throw new XsException("Unexpected respond {CMD: " + res.cmd + ", ARG:" + res.getArg() + "}");
        }
        return res;
    }

    public XsCommand execCommand(XsCommand cmd) {
        return execCommand(cmd, XsCmd.XS_CMD_NONE, XsCmd.XS_CMD_OK);
    }

    public XsCommand execCommand(XsCommand cmd, int resArg) {
        return execCommand(cmd, resArg, XsCmd.XS_CMD_OK);
    }

    public XsCommand execCommand(Map<String, Object> cmd) {
        return execCommand(new XsCommand(cmd), XsCmd.XS_CMD_NONE, XsCmd.XS_CMD_OK);
    }

    public XsCommand execCommand(Map<String, Object> cmd, int resArg) {
        return execCommand(new XsCommand(cmd), resArg, XsCmd.XS_CMD_OK);
    }

    public XsCommand execCommand(Map<String, Object> cmd, int resArg, int resCmd) {
        return execCommand(new XsCommand(cmd), resArg, resCmd);
    }

    protected void write(ArrayList<Byte> buf, int len) {
        if (len == 0 && buf.isEmpty()) {
            return;
        }
        this.check();
        WritableByteChannel channel = (WritableByteChannel) this.channel;
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocate(buf.size());
            for (int i=0;i<buf.size(); i++) {
                byteBuffer.put(buf.get(i));
            }
            channel.write(byteBuffer);
        } catch (Exception e) {
            this.close(true);
            throw new XsException("Failed to send the data to server completely REASON:" + (channel.isOpen() ? "unknown":"closed"));
        }
    }

    protected void write(ArrayList<Byte> buf) {
        write(buf, 0);
    }

    /**
     * 写入数据
     * @param buf
     * @param len
     */
    protected void write(String buf, int len) {
        //注意这里的编码选择，具体见XsCommand
        byte[] bufBytes = buf.getBytes(StandardCharsets.ISO_8859_1);
        if (len == 0 && bufBytes.length == 0){
            return;
        }

        this.check();
        WritableByteChannel channel = (WritableByteChannel) this.channel;
        try {
            channel.write(ByteBuffer.wrap(bufBytes));
        } catch (Exception e) {
            this.close(true);
            throw new XsException("Failed to send the data to server completely REASON:" + (channel.isOpen() ? "unknown":"closed"));
        }
    }

    protected void write(String buf) {
        write(buf, 0);
    }

    protected void write(byte[] buf, int len) {
        if (len == 0 && buf.length == 0){
            return;
        }

        this.check();
        WritableByteChannel channel = (WritableByteChannel) this.channel;
        try {
            channel.write(ByteBuffer.wrap(buf));
        } catch (Exception e) {
            this.close(true);
            throw new XsException("Failed to send the data to server completely REASON:" + (channel.isOpen() ? "unknown":"closed"));
        }
    }

    protected void write(byte[] buf) {
        write(buf, 0);
    }

    /**
     * 从服务器读取响应指令
     * @return 成功返回响应指令
     */
    public XsCommand getRespond() {
//        String buf = this.read(8);//正常会读取到8字节长度
        byte[] bufBytes = this.read(8);
        XsCommand res = new XsCommand((int)(bufBytes[0]&0xFF));
        res.arg1 = (int)(bufBytes[1] & 0xFF);//安全将无符号数字转换为java中的数字
        res.arg2 = (int)(bufBytes[2] & 0xFF);
        int blen1 = (int)(bufBytes[3] & 0xFF);
        int blen = ByteBuffer.wrap(bufBytes, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        res.buf = ByteBuffer.wrap(this.read(blen)).order(ByteOrder.LITTLE_ENDIAN);
        res.buf1 = ByteBuffer.wrap(this.read(blen1)).order(ByteOrder.LITTLE_ENDIAN);
        return res;
    }

    /**
     * 读取数据
     * @param len 要读入的数据长度（字节）
     * @return 成功时返回读到的字符串
     */
    protected byte[] read(int len) {
        if (len == 0) {
            return new byte[0];
//            return "";
        }
        this.check();
        ReadableByteChannel channel = (ReadableByteChannel) this.channel;
        ByteBuffer byteBuffer = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN);
//        StringBuilder stringBuilder = new StringBuilder();
        try {
            while (true) {
                int readByteNum = channel.read(byteBuffer);
                if (readByteNum == -1 || readByteNum == 0) break;;
//                stringBuilder.append(new String(byteBuffer.array(), StandardCharsets.UTF_8));
                if (readByteNum >= len) break;
//                byteBuffer.clear();
            }
        } catch (Exception e) {
            this.close(true);
            throw new RuntimeException("Failed to recv the data from server completely" + "REASON: " + (channel.isOpen() ? "unknown" : "closed"), e);
        }
        return byteBuffer.array();
//        return new String(byteBuffer.array(), StandardCharsets.UTF_8);
    }

    protected void check() {
        if (this.channel == null) {
            throw new XsException("No server connection");
        }
        if ((this.flag & BROKEN) > 0) {
            throw new XsException("Broken server connection");
        }
    }
}
