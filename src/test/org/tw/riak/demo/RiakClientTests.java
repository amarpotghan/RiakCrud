package test.org.tw.riak.demo;


import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RiakClientTests {

    private IRiakClient riakClient;

    @Before
    public void connnectDefaultRiakInstance() {
        try {
            riakClient = RiakFactory.httpClient("http://localhost:8098/riak");
        } catch (RiakException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void storeStringKeyValue() {
        Bucket bucket = null;
        try {
            bucket = riakClient.fetchBucket("TestBucket").execute();
            bucket.store("1", "Potghan").execute();
        } catch (RiakRetryFailedException e) {
            e.printStackTrace();
        }

        riakClient.shutdown();
    }
    @Test
    public void retrieveStringValue() {
        Bucket bucket = null;
        try {
            bucket = riakClient.fetchBucket("TestBucket").execute();
            System.out.print(bucket.fetch("1").execute().getValueAsString());
        } catch (RiakRetryFailedException e) {
            e.printStackTrace();
        }

        riakClient.shutdown();
    }
}
