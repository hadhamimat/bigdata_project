package models;

import junit.framework.TestCase;

/**
 * Created by Hadhami on 21/03/2016.
 */
public class ConsommationTest extends TestCase {

    public void testConvertConsommation() throws Exception {

        Consommation c = new Consommation("client43;-20578;MRS;-1665;10;1442131931232");
        assertEquals(c.getIdClient(),"client43");
        assertEquals(c.getVille(),"MRS");
        double d = 20578;
        assertEquals(c.getConso(),d);
        long l = 1442131931232L;
        assertEquals(c.getTimeStamp(),l);

    }
}