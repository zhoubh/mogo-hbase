package cn.creativearts.spark.data.dsp.moxie;

import java.io.Serializable;
import java.util.List;

public class Calls implements Serializable {
    private List<Items> items;

    public void setItems(List<Items> items) {
        this.items = items;
    }

    public List<Items> getItems() {
        return items;
    }

}