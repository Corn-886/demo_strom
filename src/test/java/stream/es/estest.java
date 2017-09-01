package stream.es;

import com.alibaba.fastjson.JSON;
import com.hankcs.hanlp.HanLP;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import pojo.ArticlePojo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by Corn on 2017/4/6.
 */
public class estest {

    public static void main(String[] argv) throws UnknownHostException {
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        ArticlePojo art=new ArticlePojo();
        art.setTitle("优麒麟 16.04.2 LTS 版本发布！");
        art.setContent("优麒麟操作系统16.04（Xenial Xerus）是一个长期支持版本，官方提供长达5年的技术支持（包括常规更新/ Bug 修复/安全升级）。计划在2021年前发布5个升级版，今日发布的16.04.2为第二个版本。\n" +
                "优麒麟 16.04.2 LTS 版本发布！\n" +
                "优麒麟16.04.2主要提高了它的稳定性和兼容性，在16.04.1的基础上进行了安全升级和部分bug修复，并提供4.8内核和新硬件支持。长期支持版会维护多个内核版本，前4.4内核分支会继续维护并不断将新内核功能移植进去。老用户可不用升级内核，新用户全新安装的16.04.2默认为4.8内核。\n" +
                "如何升级优麒麟16.04.2\n" +
                "对于已在使用优麒麟 16.04.1 的用户，在连网情况下可使用软件更新器直接在线更新。或在终端执行如下命令：sudo apt update && sudo apt upgrade。其他低版本可一级一级往上升级，直到最新长期支持版16.04.2。");
        List<String> keywordList2 = HanLP.extractKeyword(art.getContent(), 5);
        art.setKeywords(keywordList2.toString().replaceAll("[+]", ""));
        art.setDatatime(dateString());
        List<String>sumary=HanLP.extractSummary(art.getContent(),5);
        art.setNote(sumary.toString().replaceAll("[+]", ""));
        String a1= JSON.toJSON(art).toString();

        IndexResponse indexResponse = client
                .prepareIndex()
                .setIndex("test")
                .setType("type1").setSource(a1)
                .get();
        client.close();
        System.out.println(indexResponse.isCreated());

    }
    public static String dateString(){
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
        java.util.Date date=new java.util.Date();
        String str=sdf.format(date);
        return  str;
    }
}
