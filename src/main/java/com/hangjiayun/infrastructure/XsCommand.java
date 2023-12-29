package com.hangjiayun.infrastructure;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

/**
 * 发送到服务端的命令数据包
 */
public class XsCommand extends XsComponent{
    /**
     * 命令代码
     * 通常是与定义常量XS_CMD_xxx，取值范围0~255
     */
    public int cmd = XsCmd.XS_CMD_NONE;
    /**
     * 参数1，取值范围0~255，具体含义根据不同的CMD而变化
     */
    public int arg1 = 0;
    /**
     * 参数2，取值范围0~255，常用于存储value no，具体参照不同CMD而确定
     */
    public int arg2 = 0;
    /**
     * 主数据内容，最长2GB（php版本中标注的最大是2GB，但看实现的话更准确的应该是2GB-1bit，也就是2^32字节减去1bit）
     * 该字段字符串是一个自定义封包得到的字符串，无论是在获取到服务端数据后赋值给该字段，还是在本地客户端构建传递数据赋值到该字段，都请使用ISO_8859_x
     * 单字节编码与字节序列形式进行转换。例如：从服务端收到读取的字节数据后，使用该编码把字节数据转换成字符串赋值到该字段；在本地客户端构建传递的数据时，
     * 构建的数据请从字节序列形式构建成该字符串，并赋值到该字段。
     */
    public ByteBuffer buf;
    /**
     * 辅助数据，最长255字节
     * 该字段字符串是一个自定义封包得到的字符串，无论是在获取到服务端数据后赋值给该字段，还是在本地客户端构建传递数据赋值到该字段，都请使用ISO_8859_x
     * 单字节编码与字节序列形式进行转换。例如：从服务端收到读取的字节数据后，使用该编码把字节数据转换成字符串赋值到该字段；在本地客户端构建传递的数据时，
     * 构建的数据请从字节序列形式构建成该字符串，并赋值到该字段。
     */
    public ByteBuffer buf1;

    /**
     * 相比php版本，这里调整为传入Map类型，以方便在使用场景下方便构建参数
     * @param cmd
     */
    public XsCommand(Map<String, Object> cmd) {
        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
            try {
                Field field = this.getClass().getDeclaredField(entry.getKey());
                field.setAccessible(true);
                Class<?> fieldType = field.getType();
                if (this.isAssignable(fieldType, entry.getValue().getClass())) {
                    field.set(this, entry.getValue());
                }
            } catch (NoSuchFieldException|IllegalAccessException e) {
                continue;
            }
        }
        if (this.buf1 == null) {
            this.buf1 = ByteBuffer.allocate(0);
        }
        if (this.buf == null) {
            this.buf = ByteBuffer.allocate(0);
        }
    }

    /**
     * 判断一个类型from的值能否赋值给另一个类型to
     * @param to 被赋值的变量类型所代表的Class对象
     * @param from 赋值的变量类型所代表的Class对象
     * @return
     */
    private boolean isAssignable(Class<?> to, Class<?> from) {
        if (to.isAssignableFrom(from)) return true;
        if (!from.isPrimitive() && to.isPrimitive()) {
            //如果不是原始类，那么找出其基础的原始类
            try {
                Field field = from.getField("TYPE");
                Class<?> fromPrimitiveClass = (Class<?>) field.get(null);
                if (fromPrimitiveClass == to) return true;
            } catch (Exception e) {

            }
        }
        return false;
    }

    public XsCommand(int cmd, int arg1, int arg2, ByteBuffer buf, ByteBuffer buf1) {
        this.cmd = cmd;
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.buf = buf;
        this.buf1 = buf1;
    }

    public XsCommand(int cmd, int arg1, int arg2, ByteBuffer buf) {
        this(cmd, arg1, arg2, buf, ByteBuffer.allocate(0));
    }

    public XsCommand(int cmd, int arg1, int arg2) {
        this(cmd, arg1, arg2, ByteBuffer.allocate(0));
    }

    public XsCommand(int cmd, int arg1) {
        this(cmd, arg1, 0);
    }

    public XsCommand(int cmd) {
        this(cmd, 0);
    }


    public ByteBuffer toBytes() {
        byte[] buf1Byte = this.buf1.array();
        if (buf1Byte.length > 0xff) {
            buf1Byte = Arrays.copyOfRange(buf1Byte, 0, 0xff);
            this.buf1 = ByteBuffer.wrap(buf1Byte);
        }
        byte[] bufByte = this.buf.array();
        //以utf-8编码传递数据给服务端
        ByteBuffer buffer = ByteBuffer.allocate(8+bufByte.length+buf1Byte.length);//pack('CCCCI')4个无符号字符和1个无符号整数（每个C是1字节，每个I是4字节），注意，这与本类中定义的几个属性的数据类型范围有关系，同时C语言中的int类型所占用空间根据机器决定的，现代大多数就机器都是32位的，因此最小算是4字节
        buffer.order(ByteOrder.LITTLE_ENDIAN);//byte[]数组按索引依次由低地址到高地址，因此要使用小端字节序
        buffer.put((byte)this.cmd);
        buffer.put((byte)this.arg1);
        buffer.put((byte)this.arg2);
        buffer.put((byte)buf1Byte.length);
        buffer.putInt(bufByte.length);
        buffer.put(bufByte);
        buffer.put(buf1Byte);
        return buffer;
    }

    /**
     * 转化为封包字符串
     * @return 用于服务端交互的字符串
     */
    @Override
    public String toString() {
        return new String(this.toBytes().array(), StandardCharsets.ISO_8859_1);
    }

    /**
     * 备用方法
     * @return
     */
//    public byte[] getBytes() {
//        byte[] bytes = this.buf1.getBytes(StandardCharsets.UTF_8);
//        if (bytes.length > 0xff) {
//            this.buf1 = Arrays.toString(Arrays.copyOfRange(bytes, 0, 0xff));
//        }
////        ByteBuffer buffer = ByteBuffer.allocate(8);//pack('CCCCI')4个无符号字符和1个无符号整数（每个C是1字节，每个I是4字节），注意，这与本类中定义的几个属性的数据类型范围有关系，同时C语言中的int类型所占用空间根据机器决定的，现代大多数就机器都是32位的，因此最小算是4字节
//        byte[] buffer = new byte[8];
//        //需要通过将大空间类型截断才能确保正确
//        buffer[0] = (byte)this.cmd;
//        buffer[1] = (byte)this.arg1;
//        buffer[2] = (byte)this.arg2;
//        buffer[3] = (byte)this.buf1.getBytes(StandardCharsets.UTF_8).length;
//        byte[] bufByte = this.buf.getBytes(StandardCharsets.UTF_8);
//        byte[] buf1Byte = this.buf1.getBytes(StandardCharsets.UTF_8);
//        int bufLength = bufByte.length;
//        for (int i=4;i<8; i++) {
//            //大端字节序
//            buffer[i] = (byte)((bufLength >> (i-4)*8) & 0xFF);
//        }
//        byte[] retByte = new byte[buffer.length + bufByte.length + buf1Byte.length];
//        System.arraycopy(buffer, 0, retByte, 0, buffer.length);
//        System.arraycopy(bufByte, 0, retByte, buffer.length, bufByte.length);
//        System.arraycopy(buf1Byte, 0, retByte, buffer.length+bufByte.length, buf1Byte.length);
//        return retByte;
//    }

    public int getArg() {
        return this.arg2 | (this.arg1 << 8);
    }

    public void setArg(int arg) {
        this.arg1 = (arg >>> 8) & 0xff;
        this.arg2 = arg & 0xff;
    }
}
