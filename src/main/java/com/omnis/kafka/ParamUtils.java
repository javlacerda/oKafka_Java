package com.omnis.kafka;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class ParamUtils {

    private ParamUtils() {
    }

    public static String getString(Map<String, Object> map, String key, String defaultValue) {
        Object v = map.get(key);

        if (v == null) {
            return defaultValue;
        }

        if (v instanceof String s) {
            return s;
        }

        return String.valueOf(v);
    }

    public static int getInt(Map<String, Object> map, String key, int defaultValue) {
        Object v = map.get(key);
        if (v == null) {
            return defaultValue;
        }

        if (v instanceof Number n) {
            return n.intValue();
        }
        
        if (v instanceof String s) {
            if (s.isBlank())
                return defaultValue;
            try {
                return Integer.parseInt(s.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Key '" + key + "' must be a numeric-compatible String, got: '" + s + "'");
            }
        }

        throw new IllegalArgumentException("Key '" + key + "' must be Number or numeric String, but was " + v.getClass().getSimpleName());
    }
    
    public static boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
        Object v = map.get(key);
        if (v == null) {
            return defaultValue;
        }

        if (v instanceof Boolean b) {
            return b;
        }

        if (v instanceof Number n) {
            return n.intValue() != 0;
        }

        if (v instanceof String s) {
            String normalized = s.trim().toLowerCase();
            switch (normalized) {
                case "true":
                case "t":
                case "yes":
                case "y":
                case "1":
                    return true;

                case "false":
                case "f":
                case "no":
                case "n":
                case "0":
                    return false;

                case "":
                    return defaultValue;
            }
            
            throw new IllegalArgumentException("Key '" + key + "' expected a boolean-compatible String, but was '" + s + "'");
        }

        throw new IllegalArgumentException("Key '" + key + "' expected Boolean, Number, or String, but was " + v.getClass().getSimpleName());
    }
    
    public static Long getLong(Map<String, Object> map, String key, Long defaultValue) {
        Object v = map.get(key);
        if (v == null)
            return defaultValue;

        // Common primitives/wrappers
        if (v instanceof Long l)
            return l;
        if (v instanceof Integer i)
            return i.longValue();
        if (v instanceof Short s)
            return s.longValue();
        if (v instanceof Byte b)
            return b.longValue();
        if (v instanceof Boolean bool)
            return bool ? 1L : 0L;

        // Big integer/decimal with exact checks
        if (v instanceof BigInteger bi) {
            if (bi.bitLength() > 63) {
                throw new IllegalArgumentException("Key '" + key + "' BigInteger out of long range: " + bi);
            }
            return bi.longValue();
        }
        if (v instanceof BigDecimal bd) {
            try {
                return bd.longValueExact(); // rejects fractional values and overflow
            } catch (ArithmeticException ex) {
                throw new IllegalArgumentException("Key '" + key + "' BigDecimal must be integral and within long range: " + bd, ex);
            }
        }

        // Other Numbers (Float/Double/custom)
        if (v instanceof Number n) {
            double d = n.doubleValue();
            if (!Double.isFinite(d)) {
                throw new IllegalArgumentException("Key '" + key + "' numeric value is not finite: " + n);
            }
            long asLong = (long) d;
            if (d != asLong) {
                throw new IllegalArgumentException("Key '" + key + "' numeric value has a fractional part: " + n);
            }
            return asLong;
        }

        // String parsing (underscores, hex 0x..., binary 0b..., decimal; allow "123.0")
        if (v instanceof String s) {
            if (s.isBlank())
                return defaultValue;
            try {
                return Long.parseLong(s.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Key '" + key + "' must be a numeric-compatible String, got: '" + s + "'");
            }
        }

        throw new IllegalArgumentException("Key '" + key + "' expected Long-compatible type but was " + v.getClass().getSimpleName());
    }
    
    @SuppressWarnings("unchecked")    
    public static List<List<String>> getListOfListOfString(Map<String, Object> map, String key, List<List<String>> defaultValue) {
        Object v = map.get(key);
        if (v == null)
            return defaultValue;

        if (!(v instanceof List<?> outer)) {
            throw new IllegalArgumentException("Key '" + key + "' must be List<List<String>>, but was " + v.getClass().getSimpleName());
        }

        // Validate shape and contents
        for (Object inner : outer) {
            if (!(inner instanceof List<?> innerList)) {
                throw new IllegalArgumentException("Key '" + key + "' must be List<List<String>> (inner element not a List)");
            }
            for (Object el : innerList) {
                if (!(el instanceof String)) {
                    throw new IllegalArgumentException("Key '" + key + "' must be List<List<String>> of String (found " + (el == null ? "null" : el.getClass().getSimpleName()) + ")");
                }
            }
        }

        // Safe to cast after validation
        return (List<List<String>>) v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getMap(Map<String, ?> map, String key, Map<String, Object> defaultValue) {
        Object v = map.get(key);
        if (v == null)
            return defaultValue;

//        if (!(v instanceof Map<?, ?> rawMap)) {
        if (!(v instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Key '" + key + "' expected Map<String, Object> but was " + v.getClass().getSimpleName());
        }
        
        return (Map<String, Object>) v;

//        Map<String, Object> result = new HashMap<>(rawMap.size());
//        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
//            Object k = entry.getKey();
//            if (!(k instanceof String s)) {
//                throw new IllegalArgumentException("Key '" + key + "' contains non-String map key: " + (k == null ? "null" : k.getClass().getSimpleName()));
//            }
//            result.put(s, entry.getValue());
//        }
//
//        return Collections.unmodifiableMap(result);
    }
}
