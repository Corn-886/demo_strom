package kafka;

/**
 * Created by Corn on 2017/4/6.
 */

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;


public class MessageScheme implements Scheme {


    public List<Object> deserialize(ByteBuffer byteBuffer) {
        //从kafka中读取的值直接序列化为UTF-8的str
        String mString = getString(byteBuffer);
        return new Values(mString);

    }

    public static String getString(ByteBuffer buffer) {
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;
        try {
            charset = Charset.forName("UTF-8");
            decoder = charset.newDecoder();
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
            return charBuffer.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return "error";
        }
    }

    public Fields getOutputFields() {
        // TODO Auto-generated method stub
        return new Fields("msg");
    }
}
