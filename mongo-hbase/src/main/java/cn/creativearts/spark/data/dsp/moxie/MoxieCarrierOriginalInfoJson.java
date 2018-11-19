package cn.creativearts.spark.data.dsp.moxie;

import java.io.Serializable;
import java.util.List;

public class MoxieCarrierOriginalInfoJson implements Serializable {


    private List<Calls> calls;

    public List<Calls> getCalls() {
        return calls;
    }

    public void setCalls(List<Calls> calls) {
        this.calls = calls;
    }
}