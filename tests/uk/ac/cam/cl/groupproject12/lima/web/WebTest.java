package uk.ac.cam.cl.groupproject12.lima.web;

import org.testng.annotations.Test;

public class WebTest {
    @Test
    public void updateRouterStatus(){
        Web.newJob("127.0.0.1",12345678, 4);
        Web.updateJob("127.0.0.1",12345678, true);
    }

}
