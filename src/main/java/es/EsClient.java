package es;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Corn on 2017/4/8.
 */
public class EsClient {
    private static final String es_adress = "192.168.184.128";

    public static boolean saveToES(String JSON) throws UnknownHostException {
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(es_adress), 9300));

        IndexResponse indexResponse = client
                .prepareIndex()
                .setIndex("sym")
                .setType("article").setSource(JSON)
                .get();
        client.close();

        return indexResponse.isCreated();
    }


}
