package com.conviva.clickhouse;

import java.util.HashMap;
import java.util.Map;

public class Zee {

    public static final char DEFAULT_SEPARATOR = '\t';
    public static final String DEFAULT_LINE_END = "\n";
    public static final String quote = "'";


    private String platform;
    private long etl_tstamp;
    private long collector_tstamp;
    private long dvce_created_tstamp;
    private String event;
    private String event_id;
    private String name_tracker;
    private String v_tracker;
    private String v_collector;
    private String v_etl;
    private String user_ipaddress;
    private String network_userid;
    private String geo_country;
    private String geo_region;
    private String geo_city;
    private String geo_zipcode;
    private String geo_latitude;
    private String geo_longitude;
    private String geo_region_name;
    private String ip_isp;
    private String ip_organization;
    private String ip_netspeed;
    private String useragent;
    private String br_lang;
    private String os_timezone;
    private long dvce_screenwidth;
    private long dvce_screenheight;
    private String geo_timezone;
    private long dvce_sent_tstamp;
    private long derived_tstamp;
    private String event_vendor;
    private String event_name;
    private String event_format;
    private String event_version;
    private long load_tstamp;
    private Map<String, String> contexts_com_conviva_clid_schema_1_0_1 = new HashMap<>();
    private Map<String, String> contexts_com_conviva_device_context_1_0_0 = new HashMap<>();
    private Map<String, String> unstruct_event_com_conviva_periodic_heartbeat_1_0_0 = new HashMap<>();
    private Map<String, String> contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_2 = new HashMap<>();
    private Map<String, String> contexts_com_snowplowanalytics_snowplow_client_session_1_0_1 = new HashMap<>();
    private Map<String, String> contexts_com_snowplowanalytics_mobile_screen_1_0_0 = new HashMap<>();
    private Map<String, String> contexts_com_snowplowanalytics_mobile_application_1_0_0 = new HashMap<>();
    private Map<String, String> contexts_com_conviva_mobile_android_app_loadtime_1_0_1 = new HashMap<>();
    private Map<String, String> contexts_com_conviva_mobile_android_screen_loadtime_1_0_0 = new HashMap<>();
    private Map<String, String> contexts_com_conviva_m3dv_1_0_0 = new HashMap<>();


    public Zee() {
    }


    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public void setEtl_tstamp(long etl_tstamp) {
        this.etl_tstamp = etl_tstamp;
    }

    public void setCollector_tstamp(long collector_tstamp) {
        this.collector_tstamp = collector_tstamp;
    }

    public void setDvce_created_tstamp(long dvce_created_tstamp) {
        this.dvce_created_tstamp = dvce_created_tstamp;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public void setEvent_id(String event_id) {
        this.event_id = event_id;
    }

    public void setName_tracker(String name_tracker) {
        this.name_tracker = name_tracker;
    }

    public void setV_tracker(String v_tracker) {
        this.v_tracker = v_tracker;
    }

    public void setV_collector(String v_collector) {
        this.v_collector = v_collector;
    }

    public void setV_etl(String v_etl) {
        this.v_etl = v_etl;
    }

    public void setUser_ipaddress(String user_ipaddress) {
        this.user_ipaddress = user_ipaddress;
    }

    public void setNetwork_userid(String network_userid) {
        this.network_userid = network_userid;
    }

    public void setGeo_country(String geo_country) {
        this.geo_country = geo_country;
    }

    public void setGeo_region(String geo_region) {
        this.geo_region = geo_region;
    }

    public void setGeo_city(String geo_city) {
        this.geo_city = geo_city;
    }

    public void setGeo_zipcode(String geo_zipcode) {
        this.geo_zipcode = geo_zipcode;
    }

    public void setGeo_latitude(String geo_latitude) {
        this.geo_latitude = geo_latitude;
    }

    public void setGeo_longitude(String geo_longitude) {
        this.geo_longitude = geo_longitude;
    }

    public void setGeo_region_name(String geo_region_name) {
        this.geo_region_name = geo_region_name;
    }

    public void setIp_isp(String ip_isp) {
        this.ip_isp = ip_isp;
    }

    public void setIp_organization(String ip_organization) {
        this.ip_organization = ip_organization;
    }

    public void setIp_netspeed(String ip_netspeed) {
        this.ip_netspeed = ip_netspeed;
    }

    public void setUseragent(String useragent) {
        this.useragent = useragent;
    }

    public void setBr_lang(String br_lang) {
        this.br_lang = br_lang;
    }

    public void setOs_timezone(String os_timezone) {
        this.os_timezone = os_timezone;
    }

    public void setDvce_screenwidth(long dvce_screenwidth) {
        this.dvce_screenwidth = dvce_screenwidth;
    }

    public void setDvce_screenheight(long dvce_screenheight) {
        this.dvce_screenheight = dvce_screenheight;
    }

    public void setGeo_timezone(String geo_timezone) {
        this.geo_timezone = geo_timezone;
    }

    public void setDvce_sent_tstamp(long dvce_sent_tstamp) {
        this.dvce_sent_tstamp = dvce_sent_tstamp;
    }

    public void setDerived_tstamp(long derived_tstamp) {
        this.derived_tstamp = derived_tstamp;
    }

    public void setEvent_vendor(String event_vendor) {
        this.event_vendor = event_vendor;
    }

    public void setEvent_name(String event_name) {
        this.event_name = event_name;
    }

    public void setEvent_format(String event_format) {
        this.event_format = event_format;
    }

    public void setEvent_version(String event_version) {
        this.event_version = event_version;
    }

    public void setLoad_tstamp(long load_tstamp) {
        this.load_tstamp = load_tstamp;
    }

    public void setContexts_com_conviva_clid_schema_1_0_1(Map<String, String> contexts_com_conviva_clid_schema_1_0_1) {
        this.contexts_com_conviva_clid_schema_1_0_1 = contexts_com_conviva_clid_schema_1_0_1;
    }

    public void setContexts_com_conviva_device_context_1_0_0(Map<String, String> contexts_com_conviva_device_context_1_0_0) {
        this.contexts_com_conviva_device_context_1_0_0 = contexts_com_conviva_device_context_1_0_0;
    }

    public void setUnstruct_event_com_conviva_periodic_heartbeat_1_0_0(Map<String, String> unstruct_event_com_conviva_periodic_heartbeat_1_0_0) {
        this.unstruct_event_com_conviva_periodic_heartbeat_1_0_0 = unstruct_event_com_conviva_periodic_heartbeat_1_0_0;
    }

    public void setContexts_com_snowplowanalytics_snowplow_mobile_context_1_0_2(Map<String, String> contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_2) {
        this.contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_2 = contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_2;
    }

    public void setContexts_com_snowplowanalytics_snowplow_client_session_1_0_1(Map<String, String> contexts_com_snowplowanalytics_snowplow_client_session_1_0_1) {
        this.contexts_com_snowplowanalytics_snowplow_client_session_1_0_1 = contexts_com_snowplowanalytics_snowplow_client_session_1_0_1;
    }

    public void setContexts_com_snowplowanalytics_mobile_screen_1_0_0(Map<String, String> contexts_com_snowplowanalytics_mobile_screen_1_0_0) {
        this.contexts_com_snowplowanalytics_mobile_screen_1_0_0 = contexts_com_snowplowanalytics_mobile_screen_1_0_0;
    }

    public void setContexts_com_snowplowanalytics_mobile_application_1_0_0(Map<String, String> contexts_com_snowplowanalytics_mobile_application_1_0_0) {
        this.contexts_com_snowplowanalytics_mobile_application_1_0_0 = contexts_com_snowplowanalytics_mobile_application_1_0_0;
    }

    public void setContexts_com_conviva_mobile_android_app_loadtime_1_0_1(Map<String, String> contexts_com_conviva_mobile_android_app_loadtime_1_0_1) {
        this.contexts_com_conviva_mobile_android_app_loadtime_1_0_1 = contexts_com_conviva_mobile_android_app_loadtime_1_0_1;
    }

    public void setContexts_com_conviva_mobile_android_screen_loadtime_1_0_0(Map<String, String> contexts_com_conviva_mobile_android_screen_loadtime_1_0_0) {
        this.contexts_com_conviva_mobile_android_screen_loadtime_1_0_0 = contexts_com_conviva_mobile_android_screen_loadtime_1_0_0;
    }

    public void setContexts_com_conviva_m3dv_1_0_0(Map<String, String> contexts_com_conviva_m3dv_1_0_0) {
        this.contexts_com_conviva_m3dv_1_0_0 = contexts_com_conviva_m3dv_1_0_0;
    }


    public String toSparkTSV() {
        StringBuilder sb = new StringBuilder();
        sb.append(platform).append(DEFAULT_SEPARATOR)
                .append(etl_tstamp).append(DEFAULT_SEPARATOR)
                .append(collector_tstamp).append(DEFAULT_SEPARATOR)
                .append(dvce_created_tstamp).append(DEFAULT_SEPARATOR)
                .append(event).append(DEFAULT_SEPARATOR)
                .append(event_id).append(DEFAULT_SEPARATOR)
                .append(name_tracker).append(DEFAULT_SEPARATOR)
                .append(v_tracker).append(DEFAULT_SEPARATOR)
                .append(v_collector).append(DEFAULT_SEPARATOR)
                .append(v_etl).append(DEFAULT_SEPARATOR)
                .append(user_ipaddress).append(DEFAULT_SEPARATOR)
                .append(network_userid).append(DEFAULT_SEPARATOR)
                .append(geo_country).append(DEFAULT_SEPARATOR)
                .append(geo_region).append(DEFAULT_SEPARATOR)
                .append(geo_city).append(DEFAULT_SEPARATOR)
                .append(geo_zipcode).append(DEFAULT_SEPARATOR)
                .append(geo_latitude).append(DEFAULT_SEPARATOR)
                .append(geo_longitude).append(DEFAULT_SEPARATOR)
                .append(geo_region_name).append(DEFAULT_SEPARATOR)
                .append(ip_isp).append(DEFAULT_SEPARATOR)
                .append(ip_organization).append(DEFAULT_SEPARATOR)
                .append(ip_netspeed).append(DEFAULT_SEPARATOR)
                .append(useragent).append(DEFAULT_SEPARATOR)
                .append(br_lang).append(DEFAULT_SEPARATOR)
                .append(os_timezone).append(DEFAULT_SEPARATOR)
                .append(dvce_screenwidth).append(DEFAULT_SEPARATOR)
                .append(dvce_screenheight).append(DEFAULT_SEPARATOR)
                .append(geo_timezone).append(DEFAULT_SEPARATOR)
                .append(dvce_sent_tstamp).append(DEFAULT_SEPARATOR)
                .append(derived_tstamp).append(DEFAULT_SEPARATOR)
                .append(event_vendor).append(DEFAULT_SEPARATOR)
                .append(event_name).append(DEFAULT_SEPARATOR)
                .append(event_format).append(DEFAULT_SEPARATOR)
                .append(event_version).append(DEFAULT_SEPARATOR)
                .append(load_tstamp).append(DEFAULT_SEPARATOR)
                .append(contexts_com_conviva_clid_schema_1_0_1.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR)
                .append(contexts_com_conviva_device_context_1_0_0.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR)
                .append(unstruct_event_com_conviva_periodic_heartbeat_1_0_0.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR)
                .append(contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_2.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR)
                .append(contexts_com_snowplowanalytics_snowplow_client_session_1_0_1.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR)
                .append(contexts_com_snowplowanalytics_mobile_screen_1_0_0.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR)
                .append(contexts_com_snowplowanalytics_mobile_application_1_0_0.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR)
                .append(contexts_com_conviva_mobile_android_app_loadtime_1_0_1.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR)
                .append(contexts_com_conviva_mobile_android_screen_loadtime_1_0_0.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR)
                .append(contexts_com_conviva_m3dv_1_0_0.toString().replaceAll("=", ":")).append(DEFAULT_SEPARATOR);


        return sb.toString();
    }

    static String buildClickhouseStrArray(String[] strings_column) {

        if (strings_column != null && strings_column.length > 0) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < strings_column.length; i++) {
                sb.append(quote).append(strings_column[i]).append(quote);
                if (i != strings_column.length - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
            return sb.toString();
        } else {
            return "[]";
        }
    }
}
