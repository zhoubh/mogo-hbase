package cn.creativearts.spark.data.dsp.moxie;

import java.io.Serializable;

public class Items implements Serializable {

    private String details_id;
    private String dial_type;
    private String duration;
    private int fee;
    private String location;
    private String location_type;
    private String peer_number;
    private String time;

    public String getDetails_id() {
        return details_id;
    }

    public void setDetails_id(String details_id) {
        this.details_id = details_id;
    }

    public String getDial_type() {
        return dial_type;
    }

    public void setDial_type(String dial_type) {
        this.dial_type = dial_type;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public int getFee() {
        return fee;
    }

    public void setFee(int fee) {
        this.fee = fee;
    }


    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLocation_type() {
        return location_type;
    }

    public void setLocation_type(String location_type) {
        this.location_type = location_type;
    }

    public String getPeer_number() {
        return peer_number;
    }

    public void setPeer_number(String peer_number) {
        this.peer_number = peer_number;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}