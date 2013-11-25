package test.org.tw.riak.demo;


import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RiakClientTests {

    private IRiakClient riakClient;
    private Bucket bucket;

    @Before
    public void connnectDefaultRiakInstance() {
        try {
            riakClient = RiakFactory.httpClient("http://localhost:8098/riak");
            bucket = riakClient.fetchBucket("TestBucket").execute();
        } catch (RiakException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void storeStringKeyValue() {
        try {
            bucket.store("1", "Potghan").execute();
        } catch (RiakRetryFailedException e) {
            fail("Exception while inserting...please check stack trace");
            e.printStackTrace();
        }
        riakClient.shutdown();
    }

    @Test
    public void retrieveStringValue() {
        try {
            assertEquals("Potghan", bucket.fetch("1").execute().getValueAsString());
        } catch (RiakRetryFailedException e) {
            fail("Exception while retrieving...please check stack trace");
            e.printStackTrace();
        }
        riakClient.shutdown();
    }

    @Test
    public void storeSerializedJson() {
        Person person = new Person();
        person.name = "Amar";
        person.organisation = "Thoughtworks";
        try {
            bucket.store("amarp", person).execute();
        } catch (RiakRetryFailedException e) {
            fail("Exception while inserting...please check stack trace");
            e.printStackTrace();
        }
        riakClient.shutdown();
    }

    @Test
    public void retrieveSerializedJson() {
        try {
            Person result = bucket.fetch("amarp", Person.class).execute();
            assertEquals("Thoughtworks", result.organisation);
        } catch (RiakRetryFailedException e) {
            fail("Exception while retrieving...please check stack trace");
            e.printStackTrace();
        }
        riakClient.shutdown();
    }
}

class Person {
    public String name;
    public String organisation;
}
