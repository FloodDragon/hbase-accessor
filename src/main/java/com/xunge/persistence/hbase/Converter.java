package com.xunge.persistence.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import com.xunge.persistence.hbase.api.TypeConverter;

/**
 * 类型转换器(暂持支持基本类型和对象)
 *
 * @author stereo
 * @version 2014.2.25 一版
 */
public final class Converter {

    private static final Log LOG = LogFactory.getLog(Converter.class);

    private final static Map<Class<?>, TypeConverter<?>> CONVERTERS = new HashMap<Class<?>, TypeConverter<?>>();

    static {
        // 注册转换器
        registerType(new BigDecimalConverter());
        registerType(new BigIntegerConverter());
        registerType(new BlobConverter());
        registerType(new BooleanConverter());
        registerType(new ByteArrayConverter());
        registerType(new DateConverter());
        registerType(new DoubleConverter());
        registerType(new FloatConverter());
        registerType(new IntConverter());
        registerType(new LongConverter());
        registerType(new ShortConverter());
        registerType(new StringConverter());
        registerType(new ObjectConverter());
    }

    public static void registerType(TypeConverter<?> converter) {
        for (Class<?> cls : converter.getTypes()) {
            Converter.CONVERTERS.put(cls, converter);
        }
    }

    public static TypeConverter<?> getConverter(Class<?> cls) {
        Class<?> superclass = null;
        if (null != CONVERTERS.get(cls))
            return CONVERTERS.get(cls);
        else if (null != (superclass = cls.getSuperclass())
                && null != CONVERTERS.get(superclass))
            return CONVERTERS.get(superclass);
        else {
            Class<?>[] inters = cls.getInterfaces();
            if (inters != null || inters.length != 0)
                for (Class<?> c : inters)
                    if (null != CONVERTERS.get(c))
                        return CONVERTERS.get(c);
            throw new RuntimeException("No type found for class");
        }
    }

    /**
     * 基本类型转化成二进制
     *
     * @param object
     * @return
     */
    public static byte[] toBytes(Object object) {
        return ((TypeConverter<Object>) getConverter(object.getClass()))
                .toBytes(object);
    }

    /**
     * 二进制转化成基本类型
     *
     * @param value
     * @param c
     * @return
     */
    public static <U> U fromBytes(byte[] value, Class<U> c) {
        return (U) getConverter(c).fromBytes(value);
    }

    protected static class BigDecimalConverter implements
            TypeConverter<BigDecimal> {

        @Override
        public BigDecimal fromBytes(byte[] t) {
            return new BigDecimal(Bytes.toString(t));
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{BigDecimal.class};
        }

        @Override
        public byte[] toBytes(BigDecimal t) {
            return Bytes.toBytes(t.toPlainString());
        }
    }

    protected static class BigIntegerConverter implements
            TypeConverter<BigInteger> {

        @Override
        public BigInteger fromBytes(byte[] t) {
            return new BigInteger(t);
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{BigInteger.class};
        }

        @Override
        public byte[] toBytes(BigInteger t) {
            return t.toByteArray();
        }
    }

    protected static class BlobConverter implements TypeConverter<Blob> {

        @Override
        public Blob fromBytes(byte[] t) {
            try {
                return new SerialBlob(t);
            } catch (SerialException e) {
                throw new RuntimeException(e);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{Blob.class};
        }

        @Override
        public byte[] toBytes(Blob t) {
            InputStream binaryStream;
            try {
                binaryStream = t.getBinaryStream();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream(
                        (int) t.length());
                byte[] buffer = new byte[1024];
                int num = -1;
                while ((num = binaryStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, num);
                }
                binaryStream.close();
                return outputStream.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected static class BooleanConverter implements TypeConverter<Boolean> {

        @Override
        public Boolean fromBytes(byte[] t) {
            return Bytes.toBoolean(t);
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{Boolean.class, Boolean.TYPE};
        }

        @Override
        public byte[] toBytes(Boolean t) {
            return Bytes.toBytes(t);
        }
    }

    protected static class ByteArrayConverter implements TypeConverter<byte[]> {

        @Override
        public byte[] fromBytes(byte[] t) {
            return t;
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{byte[].class};
        }

        @Override
        public byte[] toBytes(byte[] t) {
            return t;
        }
    }

    protected static class DateConverter implements TypeConverter<Date> {

        @Override
        public Date fromBytes(byte[] t) {
            return new Date(Bytes.toLong(t));
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{Date.class, java.sql.Date.class};
        }

        @Override
        public byte[] toBytes(Date t) {
            return Bytes.toBytes(t.getTime());
        }
    }

    protected static class DoubleConverter implements TypeConverter<Double> {

        @Override
        public Double fromBytes(byte[] t) {
            return Bytes.toDouble(t);
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{Double.class, Double.TYPE};
        }

        @Override
        public byte[] toBytes(Double t) {
            return Bytes.toBytes(t);
        }
    }

    protected static class FloatConverter implements TypeConverter<Float> {

        @Override
        public Float fromBytes(byte[] t) {
            return Bytes.toFloat(t);
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{Float.class, Float.TYPE};
        }

        @Override
        public byte[] toBytes(Float t) {
            return Bytes.toBytes(t);
        }
    }

    protected static class IntConverter implements TypeConverter<Integer> {

        @Override
        public Integer fromBytes(byte[] t) {
            return Bytes.toInt(t);
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{Integer.class, Integer.TYPE};
        }

        @Override
        public byte[] toBytes(Integer t) {
            return Bytes.toBytes(t);
        }
    }

    protected static class LongConverter implements TypeConverter<Long> {

        @Override
        public Long fromBytes(byte[] t) {
            return Bytes.toLong(t);
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{Long.class, Long.TYPE};
        }

        @Override
        public byte[] toBytes(Long t) {
            return Bytes.toBytes(t);
        }
    }

    protected static class ShortConverter implements TypeConverter<Short> {

        @Override
        public Short fromBytes(byte[] t) {
            return Bytes.toShort(t);
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{Short.class, Short.TYPE};
        }

        @Override
        public byte[] toBytes(Short t) {
            return Bytes.toBytes(t);
        }
    }

    protected static class StringConverter implements TypeConverter<String> {

        @Override
        public String fromBytes(byte[] s) {
            return Bytes.toString(s);
        }

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{String.class};
        }

        @Override
        public byte[] toBytes(String s) {
            return Bytes.toBytes(s);
        }
    }

    protected static class ObjectConverter implements TypeConverter<Object> {

        @Override
        public Class<?>[] getTypes() {
            return new Class[]{Serializable.class};
        }

        @Override
        public byte[] toBytes(Object t) {
            if (!Serializable.class.isInstance(t))
                throw new RuntimeException(t.getClass().getName()
                        + " no serialization");
            ObjectOutputStream os = null;
            ByteArrayOutputStream byteArray = null;
            try {
                byteArray = new ByteArrayOutputStream();
                os = new ObjectOutputStream(byteArray);
                os.writeObject(t);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    os.close();
                    byteArray.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return byteArray.toByteArray();
        }

        @Override
        public Object fromBytes(byte[] t) {
            ObjectInputStream is = null;
            ByteArrayInputStream byteArray = null;
            Object object = null;
            try {
                byteArray = new ByteArrayInputStream(t);
                is = new ObjectInputStream(byteArray);
                object = is.readObject();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    is.close();
                    byteArray.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return object;
        }
    }

    /**
     * 暂未实现
     *
     * @param <I>
     */
    protected static class ImageConverter<I> implements TypeConverter<I> {

        @Override
        public Class<?>[] getTypes() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public byte[] toBytes(I t) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public I fromBytes(byte[] t) {
            // TODO Auto-generated method stub
            return null;
        }
    }
}