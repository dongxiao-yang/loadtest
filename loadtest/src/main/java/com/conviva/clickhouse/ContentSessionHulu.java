package com.conviva.clickhouse;

import java.util.HashMap;
import java.util.Map;

public class ContentSessionHulu {

    public static final char DEFAULT_SEPARATOR = '\t';
    public static final String DEFAULT_LINE_END = "\n";
    public static final String quote = "'";


    public String version;
    public int customerId;
    public String clientId;
    public int sessionId;
    public int segmentId;
    public String datasourceId;
    public int isAudienceOnly;
    public int isAd;
    public String assetName;
    public String streamUrl = "";
    public int contentLengthMs;
    public String ipType;
    public long geo_continent;
    public long geo_country;
    public long geo_state;
    public long geo_city;
    public int geo_dma;
    public int geo_asn;
    public int geo_isp;
    public String geo_postalCode;
    public long life_firstReceivedTimeMs;
    public long life_latestReceivedTimeMs;
    public int life_sessionTimeMs;
    public int life_joinTimeMs;
    public int life_playingTimeMs;
    public int life_bufferingTimeMs;
    public int life_networkBufferingTimeMs;
    public float life_rebufferingRatioPct;
    public float life_networkRebufferingRatioPct;
    public int life_averageBitrateKbps;
    public int life_seekJoinTimeMs;
    public int life_seekJoinCount;
    public int life_bufferingEvents;
    public int life_networkRebufferingEvents;
    public int life_bitrateKbps;
    public int life_contentWatchedTimeMs;
    public float life_contentWatchedPct;
    public int life_averageFrameRate;
    public int life_renderingQuality;
    //    public int[] life_resourceIds = new int[0];
//    public String[] life_cdns = new String[0];
//    public int[] life_fatalErrorResourceIds = new int[0];
//    public String[] life_fatalErrorCdns = new String[0];
//    public int[] life_latestErrorResourceId = new int[0];
    public String life_resourceIds = "[]";
    public String life_cdns = "[]";
    public String life_fatalErrorResourceIds = "[]";
    public String life_fatalErrorCdns = "[]";
    public int life_latestErrorResourceId;
    public String life_latestErrorCdn;
//    public String[] life_latestErrorCdn = new String[0];
//    public int[] life_joinResourceIds = new int[0];
//    public String[] life_joinCdns = new String[0];

    public String life_joinResourceIds = "[]";
    public String life_joinCdns = "[]";

    public String life_lastJoinCdn = "";
    public String life_lastCdn = "";
    public int life_lastJoinResourceId;
    public int life_isVideoPlaybackFailure;
    public int life_isVideoStartFailure;
    public int life_hasJoined;
    public int life_isVideoPlaybackFailureBusiness;
    public int life_isVideoPlaybackFailureTech;
    public int life_isVideoStartFailureBusiness;
    public int life_isVideoStartFailureTech;
    //    public String[] life_videoPlaybackFailureErrorsBusiness = new String[0];
//    public String[] life_videoPlaybackFailureErrorsTech = new String[0];
//    public String[] life_videoStartFailureErrorsBusiness = new String[0];
//    public String[] life_videoStartFailureErrorsTech = new String[0];
    public String life_videoPlaybackFailureErrorsBusiness = "[]";
    public String life_videoPlaybackFailureErrorsTech = "[]";
    public String life_videoStartFailureErrorsBusiness = "[]";
    public String life_videoStartFailureErrorsTech = "[]";
    public int life_exitDuringPreRoll;
    public int life_waitTimePrerollExitMs;
    public String life_lastCDNGroupId = "";
    public String life_lastCDNEdgeServer = "";
    public long interval_startTimeMs;

    public int switch_resourceId;
    public String switch_cdn;
    public int switch_justJoined;
    public int switch_hasJoined;
    public int switch_justJoinedAndLifeJoinTimeMsIsAccurate;
    public int switch_isEndedPlay;
    public int switch_isEnded;
    public int switch_isEndedPlayAndLifeAverageBitrateKbpsGT0;
    public int switch_isVideoStartFailure;
    public String switch_videoStartFailureErrors="[]";
    public int switch_isExitBeforeVideoStart;
    public int switch_isVideoPlaybackFailure;
    public int switch_isVideoStartSave;
    public String switch_videoPlaybackFailureErrors="[]";
    public int switch_isAttempt;
    public int switch_playingTimeMs;
    public int switch_rebufferingTimeMs;
    public int switch_networkRebufferingTimeMs;
    public int switch_rebufferingDuringAdsMs;
    public int switch_adRelatedBufferingMs;
    public long switch_bitrateBytes;
    public int switch_bitrateTimeMs;
    public int switch_framesLoaded;
    public int switch_framesPlayingTimeMs;
    public int switch_seekJoinTimeMs;
    public int switch_seekJoinCount;
    public String switch_pcpBuckets1Min = "[]";
    public long switch_pcpIntervals;
    public int switch_rebufferingTimeMsRaw;
    public int switch_networkRebufferingTimeMsRaw;
    public int switch_isVideoPlaybackFailureBusiness;
    public int switch_isVideoPlaybackFailureTech;
    public int switch_isVideoStartFailureBusiness;
    public int switch_isVideoStartFailureTech;
    public String switch_videoPlaybackFailureErrorsBusiness="[]";
    public String switch_videoPlaybackFailureErrorsTech = "[]";
    public String switch_videoStartFailureErrorsBusiness = "[]";
    public String switch_videoStartFailureErrorsTech = "[]";
    public int switch_adRequested;
    public int bucket_sessionTimeMs;
    public int bucket_joinTimeMs;
    public int bucket_playingTimeMs;
    public int bucket_bufferingTimeMs;
    public int bucket_networkBufferingTimeMs;
    public float bucket_rebufferingRatioPct;
    public float bucket_networkRebufferingRatioPct;
    public int bucket_averageBitrateKbps;
    public int bucket_seekJoinTimeMs;
    public int bucket_averageFrameRate;
    public float bucket_contentWatchedPct;
    public String m3_dv_hwt;
    public String m3_dv_mrk;

    public String c3_video_isAd;
    public String c3_ad_id;
    public String c3_ad_position;
    public String c3_ad_system;
    public String c3_ad_technology;
    public String c3_ad_isSlate;
    public String c3_ad_adStitcher;
    public String c3_ad_creativeId;
    public String c3_ad_breakId;
    public String c3_ad_contentAssetName;
    public String c3_pt_ver;
    public String c3_device_ua;
    public String c3_adaptor_type;
    public String c3_protocol_level;
    public String c3_protocol_pure;
    public String c3_pt_os_ver;
    public String c3_device_manufacturer;
    public String c3_device_brand;
    public String c3_device_model;
    public String c3_device_conn;
    public String c3_device_ver;
    public String c3_go_algoid;
    public String c3_player_name;
    public String c3_de_rs_raw;
    public String c3_de_bitr;
    public String c3_de_rsid;
    public String c3_de_cdn;
    public String c3_de_rs;
    public String c3_device_cver;
    public String c3_device_cver_bld;
    public String c3_video_isLive;
    public String c3_viewer_id;
    public String c3_client_hwType;
    public String c3_client_osname;
    public String c3_client_manufacturer;
    public String c3_client_brand;
    public String c3_client_marketingName;
    public String c3_client_model;
    public String c3_client_osv;
    public String c3_client_osf;
    public String c3_client_br;
    public String c3_client_brv;
    public String m3_dv_mnf;
    public String m3_dv_n;
    public String m3_dv_os;
    public String m3_dv_osv;
    public String m3_dv_osf;
    public String m3_dv_br;
    public String m3_dv_brv;
    public String m3_dv_fw;
    public String m3_dv_fwv;
    public String m3_dv_mod;
    public String m3_dv_vnd;
    public String m3_net_t;
    public String c3_protocol_type;
    public String c3_device_type;
    public String c3_pt_os;
    public String c3_ft_os;
    public String c3_framework;
    public String c3_framework_ver;
    public String c3_pt_br;
    public String c3_pt_br_ver;
    public String c3_br_v;
    public String appVersion;
    public String network;
    public String stormflowId;
    public String contentType;
    public String m3_dv_cat;
    public String c3_cp_an;

    public String huluPlayerFrameworkName;
    public String liveSignalProvider;
    public String clientFeatureTags;
    public String channel;
    public String huluPlayerFrameworkVersion;
    public String startType;
    public String plt;
    public String DevOps;
    public String conid;
    public String stormflow;

    public Map<String, String> tags = new HashMap<>();

    public ContentSessionHulu(){}


    public void setVersion(String version) {
        this.version = version;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public void setSegmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    public void setDatasourceId(String datasourceId) {
        this.datasourceId = datasourceId;
    }

    public void setIsAudienceOnly(int isAudienceOnly) {
        this.isAudienceOnly = isAudienceOnly;
    }

    public void setIsAd(int isAd) {
        this.isAd = isAd;
    }

    public void setAssetName(String assetName) {
        this.assetName = assetName;
    }

    public void setStreamUrl(String streamUrl) {
        this.streamUrl = streamUrl;
    }

    public void setContentLengthMs(int contentLengthMs) {
        this.contentLengthMs = contentLengthMs;
    }

    public void setIpType(String ipType) {
        this.ipType = ipType;
    }

    public void setGeo_continent(long geo_continent) {
        this.geo_continent = geo_continent;
    }

    public void setGeo_country(long geo_country) {
        this.geo_country = geo_country;
    }

    public void setGeo_state(long geo_state) {
        this.geo_state = geo_state;
    }

    public void setGeo_city(long geo_city) {
        this.geo_city = geo_city;
    }

    public void setGeo_dma(int geo_dma) {
        this.geo_dma = geo_dma;
    }

    public void setGeo_asn(int geo_asn) {
        this.geo_asn = geo_asn;
    }

    public void setGeo_isp(int geo_isp) {
        this.geo_isp = geo_isp;
    }

    public void setGeo_postalCode(String geo_postalCode) {
        this.geo_postalCode = geo_postalCode;
    }

    public void setLife_firstReceivedTimeMs(long life_firstReceivedTimeMs) {
        this.life_firstReceivedTimeMs = life_firstReceivedTimeMs;
    }

    public void setLife_latestReceivedTimeMs(long life_latestReceivedTimeMs) {
        this.life_latestReceivedTimeMs = life_latestReceivedTimeMs;
    }

    public void setLife_sessionTimeMs(int life_sessionTimeMs) {
        this.life_sessionTimeMs = life_sessionTimeMs;
    }

    public void setLife_joinTimeMs(int life_joinTimeMs) {
        this.life_joinTimeMs = life_joinTimeMs;
    }

    public void setLife_playingTimeMs(int life_playingTimeMs) {
        this.life_playingTimeMs = life_playingTimeMs;
    }

    public void setLife_bufferingTimeMs(int life_bufferingTimeMs) {
        this.life_bufferingTimeMs = life_bufferingTimeMs;
    }

    public void setLife_networkBufferingTimeMs(int life_networkBufferingTimeMs) {
        this.life_networkBufferingTimeMs = life_networkBufferingTimeMs;
    }

    public void setLife_rebufferingRatioPct(float life_rebufferingRatioPct) {
        this.life_rebufferingRatioPct = life_rebufferingRatioPct;
    }

    public void setLife_networkRebufferingRatioPct(float life_networkRebufferingRatioPct) {
        this.life_networkRebufferingRatioPct = life_networkRebufferingRatioPct;
    }

    public void setLife_averageBitrateKbps(int life_averageBitrateKbps) {
        this.life_averageBitrateKbps = life_averageBitrateKbps;
    }

    public void setLife_seekJoinTimeMs(int life_seekJoinTimeMs) {
        this.life_seekJoinTimeMs = life_seekJoinTimeMs;
    }

    public void setLife_seekJoinCount(int life_seekJoinCount) {
        this.life_seekJoinCount = life_seekJoinCount;
    }

    public void setLife_bufferingEvents(int life_bufferingEvents) {
        this.life_bufferingEvents = life_bufferingEvents;
    }

    public void setLife_networkRebufferingEvents(int life_networkRebufferingEvents) {
        this.life_networkRebufferingEvents = life_networkRebufferingEvents;
    }

    public void setLife_bitrateKbps(int life_bitrateKbps) {
        this.life_bitrateKbps = life_bitrateKbps;
    }

    public void setLife_contentWatchedTimeMs(int life_contentWatchedTimeMs) {
        this.life_contentWatchedTimeMs = life_contentWatchedTimeMs;
    }

    public void setLife_contentWatchedPct(float life_contentWatchedPct) {
        this.life_contentWatchedPct = life_contentWatchedPct;
    }

    public void setLife_averageFrameRate(int life_averageFrameRate) {
        this.life_averageFrameRate = life_averageFrameRate;
    }

    public void setLife_renderingQuality(int life_renderingQuality) {
        this.life_renderingQuality = life_renderingQuality;
    }

    public void setLife_resourceIds(String life_resourceIds) {
        this.life_resourceIds = life_resourceIds;
    }

    public void setLife_cdns(String life_cdns) {
        this.life_cdns = life_cdns;
    }

    public void setLife_fatalErrorResourceIds(String life_fatalErrorResourceIds) {
        this.life_fatalErrorResourceIds = life_fatalErrorResourceIds;
    }

    public void setLife_fatalErrorCdns(String life_fatalErrorCdns) {
        this.life_fatalErrorCdns = life_fatalErrorCdns;
    }

    public void setLife_latestErrorResourceId(int life_latestErrorResourceId) {
        this.life_latestErrorResourceId = life_latestErrorResourceId;
    }

    public void setLife_latestErrorCdn(String life_latestErrorCdn) {
        this.life_latestErrorCdn = life_latestErrorCdn;
    }

    public void setLife_joinResourceIds(String life_joinResourceIds) {
        this.life_joinResourceIds = life_joinResourceIds;
    }

    public void setLife_joinCdns(String life_joinCdns) {
        this.life_joinCdns = life_joinCdns;
    }

    public void setLife_lastJoinCdn(String life_lastJoinCdn) {
        this.life_lastJoinCdn = life_lastJoinCdn;
    }

    public void setLife_lastCdn(String life_lastCdn) {
        this.life_lastCdn = life_lastCdn;
    }

    public void setLife_lastJoinResourceId(int life_lastJoinResourceId) {
        this.life_lastJoinResourceId = life_lastJoinResourceId;
    }

    public void setLife_isVideoPlaybackFailure(int life_isVideoPlaybackFailure) {
        this.life_isVideoPlaybackFailure = life_isVideoPlaybackFailure;
    }

    public void setLife_isVideoStartFailure(int life_isVideoStartFailure) {
        this.life_isVideoStartFailure = life_isVideoStartFailure;
    }

    public void setLife_hasJoined(int life_hasJoined) {
        this.life_hasJoined = life_hasJoined;
    }

    public void setLife_isVideoPlaybackFailureBusiness(int life_isVideoPlaybackFailureBusiness) {
        this.life_isVideoPlaybackFailureBusiness = life_isVideoPlaybackFailureBusiness;
    }

    public void setLife_isVideoPlaybackFailureTech(int life_isVideoPlaybackFailureTech) {
        this.life_isVideoPlaybackFailureTech = life_isVideoPlaybackFailureTech;
    }

    public void setLife_isVideoStartFailureBusiness(int life_isVideoStartFailureBusiness) {
        this.life_isVideoStartFailureBusiness = life_isVideoStartFailureBusiness;
    }

    public void setLife_isVideoStartFailureTech(int life_isVideoStartFailureTech) {
        this.life_isVideoStartFailureTech = life_isVideoStartFailureTech;
    }

    public void setLife_videoPlaybackFailureErrorsBusiness(String life_videoPlaybackFailureErrorsBusiness) {
        this.life_videoPlaybackFailureErrorsBusiness = life_videoPlaybackFailureErrorsBusiness;
    }

    public void setLife_videoPlaybackFailureErrorsTech(String life_videoPlaybackFailureErrorsTech) {
        this.life_videoPlaybackFailureErrorsTech = life_videoPlaybackFailureErrorsTech;
    }

    public void setLife_videoStartFailureErrorsBusiness(String life_videoStartFailureErrorsBusiness) {
        this.life_videoStartFailureErrorsBusiness = life_videoStartFailureErrorsBusiness;
    }

    public void setLife_videoStartFailureErrorsTech(String life_videoStartFailureErrorsTech) {
        this.life_videoStartFailureErrorsTech = life_videoStartFailureErrorsTech;
    }

    public void setLife_exitDuringPreRoll(int life_exitDuringPreRoll) {
        this.life_exitDuringPreRoll = life_exitDuringPreRoll;
    }

    public void setLife_waitTimePrerollExitMs(int life_waitTimePrerollExitMs) {
        this.life_waitTimePrerollExitMs = life_waitTimePrerollExitMs;
    }

    public void setLife_lastCDNGroupId(String life_lastCDNGroupId) {
        this.life_lastCDNGroupId = life_lastCDNGroupId;
    }

    public void setLife_lastCDNEdgeServer(String life_lastCDNEdgeServer) {
        this.life_lastCDNEdgeServer = life_lastCDNEdgeServer;
    }

    public void setInterval_startTimeMs(long interval_startTimeMs) {
        this.interval_startTimeMs = interval_startTimeMs;
    }

    public void setSwitch_resourceId(int switch_resourceId) {
        this.switch_resourceId = switch_resourceId;
    }

    public void setSwitch_cdn(String switch_cdn) {
        this.switch_cdn = switch_cdn;
    }

    public void setSwitch_justJoined(int switch_justJoined) {
        this.switch_justJoined = switch_justJoined;
    }

    public void setSwitch_hasJoined(int switch_hasJoined) {
        this.switch_hasJoined = switch_hasJoined;
    }

    public void setSwitch_justJoinedAndLifeJoinTimeMsIsAccurate(int switch_justJoinedAndLifeJoinTimeMsIsAccurate) {
        this.switch_justJoinedAndLifeJoinTimeMsIsAccurate = switch_justJoinedAndLifeJoinTimeMsIsAccurate;
    }

    public void setSwitch_isEndedPlay(int switch_isEndedPlay) {
        this.switch_isEndedPlay = switch_isEndedPlay;
    }

    public void setSwitch_isEnded(int switch_isEnded) {
        this.switch_isEnded = switch_isEnded;
    }

    public void setSwitch_isEndedPlayAndLifeAverageBitrateKbpsGT0(int switch_isEndedPlayAndLifeAverageBitrateKbpsGT0) {
        this.switch_isEndedPlayAndLifeAverageBitrateKbpsGT0 = switch_isEndedPlayAndLifeAverageBitrateKbpsGT0;
    }

    public void setSwitch_isVideoStartFailure(int switch_isVideoStartFailure) {
        this.switch_isVideoStartFailure = switch_isVideoStartFailure;
    }

    public void setSwitch_videoStartFailureErrors(String switch_videoStartFailureErrors) {
        this.switch_videoStartFailureErrors = switch_videoStartFailureErrors;
    }

    public void setSwitch_isExitBeforeVideoStart(int switch_isExitBeforeVideoStart) {
        this.switch_isExitBeforeVideoStart = switch_isExitBeforeVideoStart;
    }

    public void setSwitch_isVideoPlaybackFailure(int switch_isVideoPlaybackFailure) {
        this.switch_isVideoPlaybackFailure = switch_isVideoPlaybackFailure;
    }

    public void setSwitch_isVideoStartSave(int switch_isVideoStartSave) {
        this.switch_isVideoStartSave = switch_isVideoStartSave;
    }

    public void setSwitch_videoPlaybackFailureErrors(String switch_videoPlaybackFailureErrors) {
        this.switch_videoPlaybackFailureErrors = switch_videoPlaybackFailureErrors;
    }

    public void setSwitch_isAttempt(int switch_isAttempt) {
        this.switch_isAttempt = switch_isAttempt;
    }

    public void setSwitch_playingTimeMs(int switch_playingTimeMs) {
        this.switch_playingTimeMs = switch_playingTimeMs;
    }

    public void setSwitch_rebufferingTimeMs(int switch_rebufferingTimeMs) {
        this.switch_rebufferingTimeMs = switch_rebufferingTimeMs;
    }

    public void setSwitch_networkRebufferingTimeMs(int switch_networkRebufferingTimeMs) {
        this.switch_networkRebufferingTimeMs = switch_networkRebufferingTimeMs;
    }

    public void setSwitch_rebufferingDuringAdsMs(int switch_rebufferingDuringAdsMs) {
        this.switch_rebufferingDuringAdsMs = switch_rebufferingDuringAdsMs;
    }

    public void setSwitch_adRelatedBufferingMs(int switch_adRelatedBufferingMs) {
        this.switch_adRelatedBufferingMs = switch_adRelatedBufferingMs;
    }

    public void setSwitch_bitrateBytes(long switch_bitrateBytes) {
        this.switch_bitrateBytes = switch_bitrateBytes;
    }

    public void setSwitch_bitrateTimeMs(int switch_bitrateTimeMs) {
        this.switch_bitrateTimeMs = switch_bitrateTimeMs;
    }

    public void setSwitch_framesLoaded(int switch_framesLoaded) {
        this.switch_framesLoaded = switch_framesLoaded;
    }

    public void setSwitch_framesPlayingTimeMs(int switch_framesPlayingTimeMs) {
        this.switch_framesPlayingTimeMs = switch_framesPlayingTimeMs;
    }

    public void setSwitch_seekJoinTimeMs(int switch_seekJoinTimeMs) {
        this.switch_seekJoinTimeMs = switch_seekJoinTimeMs;
    }

    public void setSwitch_seekJoinCount(int switch_seekJoinCount) {
        this.switch_seekJoinCount = switch_seekJoinCount;
    }

    public void setSwitch_pcpBuckets1Min(String switch_pcpBuckets1Min) {
        this.switch_pcpBuckets1Min = switch_pcpBuckets1Min;
    }

    public void setSwitch_pcpIntervals(long switch_pcpIntervals) {
        this.switch_pcpIntervals = switch_pcpIntervals;
    }

    public void setSwitch_rebufferingTimeMsRaw(int switch_rebufferingTimeMsRaw) {
        this.switch_rebufferingTimeMsRaw = switch_rebufferingTimeMsRaw;
    }

    public void setSwitch_networkRebufferingTimeMsRaw(int switch_networkRebufferingTimeMsRaw) {
        this.switch_networkRebufferingTimeMsRaw = switch_networkRebufferingTimeMsRaw;
    }

    public void setSwitch_isVideoPlaybackFailureBusiness(int switch_isVideoPlaybackFailureBusiness) {
        this.switch_isVideoPlaybackFailureBusiness = switch_isVideoPlaybackFailureBusiness;
    }

    public void setSwitch_isVideoPlaybackFailureTech(int switch_isVideoPlaybackFailureTech) {
        this.switch_isVideoPlaybackFailureTech = switch_isVideoPlaybackFailureTech;
    }

    public void setSwitch_isVideoStartFailureBusiness(int switch_isVideoStartFailureBusiness) {
        this.switch_isVideoStartFailureBusiness = switch_isVideoStartFailureBusiness;
    }

    public void setSwitch_isVideoStartFailureTech(int switch_isVideoStartFailureTech) {
        this.switch_isVideoStartFailureTech = switch_isVideoStartFailureTech;
    }

    public void setSwitch_videoPlaybackFailureErrorsBusiness(String switch_videoPlaybackFailureErrorsBusiness) {
        this.switch_videoPlaybackFailureErrorsBusiness = switch_videoPlaybackFailureErrorsBusiness;
    }

    public void setSwitch_videoPlaybackFailureErrorsTech(String switch_videoPlaybackFailureErrorsTech) {
        this.switch_videoPlaybackFailureErrorsTech = switch_videoPlaybackFailureErrorsTech;
    }

    public void setSwitch_videoStartFailureErrorsBusiness(String switch_videoStartFailureErrorsBusiness) {
        this.switch_videoStartFailureErrorsBusiness = switch_videoStartFailureErrorsBusiness;
    }

    public void setSwitch_videoStartFailureErrorsTech(String switch_videoStartFailureErrorsTech) {
        this.switch_videoStartFailureErrorsTech = switch_videoStartFailureErrorsTech;
    }

    public void setSwitch_adRequested(int switch_adRequested) {
        this.switch_adRequested = switch_adRequested;
    }

    public void setBucket_sessionTimeMs(int bucket_sessionTimeMs) {
        this.bucket_sessionTimeMs = bucket_sessionTimeMs;
    }

    public void setBucket_joinTimeMs(int bucket_joinTimeMs) {
        this.bucket_joinTimeMs = bucket_joinTimeMs;
    }

    public void setBucket_playingTimeMs(int bucket_playingTimeMs) {
        this.bucket_playingTimeMs = bucket_playingTimeMs;
    }

    public void setBucket_bufferingTimeMs(int bucket_bufferingTimeMs) {
        this.bucket_bufferingTimeMs = bucket_bufferingTimeMs;
    }

    public void setBucket_networkBufferingTimeMs(int bucket_networkBufferingTimeMs) {
        this.bucket_networkBufferingTimeMs = bucket_networkBufferingTimeMs;
    }

    public void setBucket_rebufferingRatioPct(float bucket_rebufferingRatioPct) {
        this.bucket_rebufferingRatioPct = bucket_rebufferingRatioPct;
    }

    public void setBucket_networkRebufferingRatioPct(float bucket_networkRebufferingRatioPct) {
        this.bucket_networkRebufferingRatioPct = bucket_networkRebufferingRatioPct;
    }

    public void setBucket_averageBitrateKbps(int bucket_averageBitrateKbps) {
        this.bucket_averageBitrateKbps = bucket_averageBitrateKbps;
    }

    public void setBucket_seekJoinTimeMs(int bucket_seekJoinTimeMs) {
        this.bucket_seekJoinTimeMs = bucket_seekJoinTimeMs;
    }

    public void setBucket_averageFrameRate(int bucket_averageFrameRate) {
        this.bucket_averageFrameRate = bucket_averageFrameRate;
    }

    public void setBucket_contentWatchedPct(float bucket_contentWatchedPct) {
        this.bucket_contentWatchedPct = bucket_contentWatchedPct;
    }

    public void setM3_dv_hwt(String m3_dv_hwt) {
        this.m3_dv_hwt = m3_dv_hwt;
    }

    public void setM3_dv_mrk(String m3_dv_mrk) {
        this.m3_dv_mrk = m3_dv_mrk;
    }

    public void setC3_video_isAd(String c3_video_isAd) {
        this.c3_video_isAd = c3_video_isAd;
    }

    public void setC3_ad_id(String c3_ad_id) {
        this.c3_ad_id = c3_ad_id;
    }

    public void setC3_ad_position(String c3_ad_position) {
        this.c3_ad_position = c3_ad_position;
    }

    public void setC3_ad_system(String c3_ad_system) {
        this.c3_ad_system = c3_ad_system;
    }

    public void setC3_ad_technology(String c3_ad_technology) {
        this.c3_ad_technology = c3_ad_technology;
    }

    public void setC3_ad_isSlate(String c3_ad_isSlate) {
        this.c3_ad_isSlate = c3_ad_isSlate;
    }

    public void setC3_ad_adStitcher(String c3_ad_adStitcher) {
        this.c3_ad_adStitcher = c3_ad_adStitcher;
    }

    public void setC3_ad_creativeId(String c3_ad_creativeId) {
        this.c3_ad_creativeId = c3_ad_creativeId;
    }

    public void setC3_ad_breakId(String c3_ad_breakId) {
        this.c3_ad_breakId = c3_ad_breakId;
    }

    public void setC3_ad_contentAssetName(String c3_ad_contentAssetName) {
        this.c3_ad_contentAssetName = c3_ad_contentAssetName;
    }

    public void setC3_pt_ver(String c3_pt_ver) {
        this.c3_pt_ver = c3_pt_ver;
    }

    public void setC3_device_ua(String c3_device_ua) {
        this.c3_device_ua = c3_device_ua;
    }

    public void setC3_adaptor_type(String c3_adaptor_type) {
        this.c3_adaptor_type = c3_adaptor_type;
    }

    public void setC3_protocol_level(String c3_protocol_level) {
        this.c3_protocol_level = c3_protocol_level;
    }

    public void setC3_protocol_pure(String c3_protocol_pure) {
        this.c3_protocol_pure = c3_protocol_pure;
    }

    public void setC3_pt_os_ver(String c3_pt_os_ver) {
        this.c3_pt_os_ver = c3_pt_os_ver;
    }

    public void setC3_device_manufacturer(String c3_device_manufacturer) {
        this.c3_device_manufacturer = c3_device_manufacturer;
    }

    public void setC3_device_brand(String c3_device_brand) {
        this.c3_device_brand = c3_device_brand;
    }

    public void setC3_device_model(String c3_device_model) {
        this.c3_device_model = c3_device_model;
    }

    public void setC3_device_conn(String c3_device_conn) {
        this.c3_device_conn = c3_device_conn;
    }

    public void setC3_device_ver(String c3_device_ver) {
        this.c3_device_ver = c3_device_ver;
    }

    public void setC3_go_algoid(String c3_go_algoid) {
        this.c3_go_algoid = c3_go_algoid;
    }

    public void setC3_player_name(String c3_player_name) {
        this.c3_player_name = c3_player_name;
    }

    public void setC3_de_rs_raw(String c3_de_rs_raw) {
        this.c3_de_rs_raw = c3_de_rs_raw;
    }

    public void setC3_de_bitr(String c3_de_bitr) {
        this.c3_de_bitr = c3_de_bitr;
    }

    public void setC3_de_rsid(String c3_de_rsid) {
        this.c3_de_rsid = c3_de_rsid;
    }

    public void setC3_de_cdn(String c3_de_cdn) {
        this.c3_de_cdn = c3_de_cdn;
    }

    public void setC3_de_rs(String c3_de_rs) {
        this.c3_de_rs = c3_de_rs;
    }

    public void setC3_device_cver(String c3_device_cver) {
        this.c3_device_cver = c3_device_cver;
    }

    public void setC3_device_cver_bld(String c3_device_cver_bld) {
        this.c3_device_cver_bld = c3_device_cver_bld;
    }

    public void setC3_video_isLive(String c3_video_isLive) {
        this.c3_video_isLive = c3_video_isLive;
    }

    public void setC3_viewer_id(String c3_viewer_id) {
        this.c3_viewer_id = c3_viewer_id;
    }

    public void setC3_client_hwType(String c3_client_hwType) {
        this.c3_client_hwType = c3_client_hwType;
    }

    public void setC3_client_osname(String c3_client_osname) {
        this.c3_client_osname = c3_client_osname;
    }

    public void setC3_client_manufacturer(String c3_client_manufacturer) {
        this.c3_client_manufacturer = c3_client_manufacturer;
    }

    public void setC3_client_brand(String c3_client_brand) {
        this.c3_client_brand = c3_client_brand;
    }

    public void setC3_client_marketingName(String c3_client_marketingName) {
        this.c3_client_marketingName = c3_client_marketingName;
    }

    public void setC3_client_model(String c3_client_model) {
        this.c3_client_model = c3_client_model;
    }

    public void setC3_client_osv(String c3_client_osv) {
        this.c3_client_osv = c3_client_osv;
    }

    public void setC3_client_osf(String c3_client_osf) {
        this.c3_client_osf = c3_client_osf;
    }

    public void setC3_client_br(String c3_client_br) {
        this.c3_client_br = c3_client_br;
    }

    public void setC3_client_brv(String c3_client_brv) {
        this.c3_client_brv = c3_client_brv;
    }

    public void setM3_dv_mnf(String m3_dv_mnf) {
        this.m3_dv_mnf = m3_dv_mnf;
    }

    public void setM3_dv_n(String m3_dv_n) {
        this.m3_dv_n = m3_dv_n;
    }

    public void setM3_dv_os(String m3_dv_os) {
        this.m3_dv_os = m3_dv_os;
    }

    public void setM3_dv_osv(String m3_dv_osv) {
        this.m3_dv_osv = m3_dv_osv;
    }

    public void setM3_dv_osf(String m3_dv_osf) {
        this.m3_dv_osf = m3_dv_osf;
    }

    public void setM3_dv_br(String m3_dv_br) {
        this.m3_dv_br = m3_dv_br;
    }

    public void setM3_dv_brv(String m3_dv_brv) {
        this.m3_dv_brv = m3_dv_brv;
    }

    public void setM3_dv_fw(String m3_dv_fw) {
        this.m3_dv_fw = m3_dv_fw;
    }

    public void setM3_dv_fwv(String m3_dv_fwv) {
        this.m3_dv_fwv = m3_dv_fwv;
    }

    public void setM3_dv_mod(String m3_dv_mod) {
        this.m3_dv_mod = m3_dv_mod;
    }

    public void setM3_dv_vnd(String m3_dv_vnd) {
        this.m3_dv_vnd = m3_dv_vnd;
    }

    public void setM3_net_t(String m3_net_t) {
        this.m3_net_t = m3_net_t;
    }

    public void setC3_protocol_type(String c3_protocol_type) {
        this.c3_protocol_type = c3_protocol_type;
    }

    public void setC3_device_type(String c3_device_type) {
        this.c3_device_type = c3_device_type;
    }

    public void setC3_pt_os(String c3_pt_os) {
        this.c3_pt_os = c3_pt_os;
    }

    public void setC3_ft_os(String c3_ft_os) {
        this.c3_ft_os = c3_ft_os;
    }

    public void setC3_framework(String c3_framework) {
        this.c3_framework = c3_framework;
    }

    public void setC3_framework_ver(String c3_framework_ver) {
        this.c3_framework_ver = c3_framework_ver;
    }

    public void setC3_pt_br(String c3_pt_br) {
        this.c3_pt_br = c3_pt_br;
    }

    public void setC3_pt_br_ver(String c3_pt_br_ver) {
        this.c3_pt_br_ver = c3_pt_br_ver;
    }

    public void setC3_br_v(String c3_br_v) {
        this.c3_br_v = c3_br_v;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public void setStormflowId(String stormflowId) {
        this.stormflowId = stormflowId;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public void setM3_dv_cat(String m3_dv_cat) {
        this.m3_dv_cat = m3_dv_cat;
    }

    public void setC3_cp_an(String c3_cp_an) {
        this.c3_cp_an = c3_cp_an;
    }

    public void setHuluPlayerFrameworkName(String huluPlayerFrameworkName) {
        this.huluPlayerFrameworkName = huluPlayerFrameworkName;
    }

    public void setLiveSignalProvider(String liveSignalProvider) {
        this.liveSignalProvider = liveSignalProvider;
    }

    public void setClientFeatureTags(String clientFeatureTags) {
        this.clientFeatureTags = clientFeatureTags;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public void setHuluPlayerFrameworkVersion(String huluPlayerFrameworkVersion) {
        this.huluPlayerFrameworkVersion = huluPlayerFrameworkVersion;
    }

    public void setStartType(String startType) {
        this.startType = startType;
    }

    public void setPlt(String plt) {
        this.plt = plt;
    }

    public void setDevOps(String devOps) {
        DevOps = devOps;
    }

    public void setConid(String conid) {
        this.conid = conid;
    }

    public void setStormflow(String stormflow) {
        this.stormflow = stormflow;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public String toTSV() {
        StringBuilder sb = new StringBuilder();
        sb.append(version).append(DEFAULT_SEPARATOR)
                .append(customerId).append(DEFAULT_SEPARATOR)
                .append(clientId).append(DEFAULT_SEPARATOR)
                .append(sessionId).append(DEFAULT_SEPARATOR)
                .append(segmentId).append(DEFAULT_SEPARATOR)
                .append(datasourceId).append(DEFAULT_SEPARATOR)
                .append(m3_dv_hwt).append(DEFAULT_SEPARATOR)
                .append(m3_dv_mrk).append(DEFAULT_SEPARATOR)
                .append(isAudienceOnly).append(DEFAULT_SEPARATOR)
                .append(isAd).append(DEFAULT_SEPARATOR)
                .append(assetName).append(DEFAULT_SEPARATOR)
                .append(streamUrl).append(DEFAULT_SEPARATOR)
                .append(contentLengthMs).append(DEFAULT_SEPARATOR)
                .append(ipType).append(DEFAULT_SEPARATOR)
                .append(geo_continent).append(DEFAULT_SEPARATOR)
                .append(geo_country).append(DEFAULT_SEPARATOR)
                .append(geo_state).append(DEFAULT_SEPARATOR)
                .append(geo_city).append(DEFAULT_SEPARATOR)
                .append(geo_dma).append(DEFAULT_SEPARATOR)
                .append(geo_asn).append(DEFAULT_SEPARATOR)
                .append(geo_isp).append(DEFAULT_SEPARATOR)
                .append(geo_postalCode).append(DEFAULT_SEPARATOR)
                .append(life_firstReceivedTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_latestReceivedTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_sessionTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_joinTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_playingTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_bufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_networkBufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_rebufferingRatioPct).append(DEFAULT_SEPARATOR)
                .append(life_networkRebufferingRatioPct).append(DEFAULT_SEPARATOR)
                .append(life_averageBitrateKbps).append(DEFAULT_SEPARATOR)
                .append(life_seekJoinTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_seekJoinCount).append(DEFAULT_SEPARATOR)
                .append(life_bufferingEvents).append(DEFAULT_SEPARATOR)
                .append(life_networkRebufferingEvents).append(DEFAULT_SEPARATOR)
                .append(life_bitrateKbps).append(DEFAULT_SEPARATOR)
                .append(life_contentWatchedTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_contentWatchedPct).append(DEFAULT_SEPARATOR)
                .append(life_averageFrameRate).append(DEFAULT_SEPARATOR)
                .append(life_renderingQuality).append(DEFAULT_SEPARATOR)
                .append(life_resourceIds).append(DEFAULT_SEPARATOR)
                .append(life_cdns).append(DEFAULT_SEPARATOR)
                .append(life_fatalErrorResourceIds).append(DEFAULT_SEPARATOR)
                .append(life_fatalErrorCdns).append(DEFAULT_SEPARATOR)
                .append(life_latestErrorResourceId).append(DEFAULT_SEPARATOR)
                .append(life_latestErrorCdn).append(DEFAULT_SEPARATOR)
                .append(life_joinResourceIds).append(DEFAULT_SEPARATOR)
                .append(life_joinCdns).append(DEFAULT_SEPARATOR)
                .append(life_lastJoinCdn).append(DEFAULT_SEPARATOR)
                .append(life_lastCdn).append(DEFAULT_SEPARATOR)
                .append(life_lastJoinResourceId).append(DEFAULT_SEPARATOR)
                .append(life_isVideoPlaybackFailure).append(DEFAULT_SEPARATOR)
                .append(life_isVideoStartFailure).append(DEFAULT_SEPARATOR)
                .append(life_hasJoined).append(DEFAULT_SEPARATOR)
                .append(life_isVideoPlaybackFailureBusiness).append(DEFAULT_SEPARATOR)
                .append(life_isVideoPlaybackFailureTech).append(DEFAULT_SEPARATOR)
                .append(life_isVideoStartFailureBusiness).append(DEFAULT_SEPARATOR)
                .append(life_isVideoStartFailureTech).append(DEFAULT_SEPARATOR)
                .append(life_videoPlaybackFailureErrorsBusiness).append(DEFAULT_SEPARATOR)
                .append(life_videoPlaybackFailureErrorsTech).append(DEFAULT_SEPARATOR)
                .append(life_videoStartFailureErrorsBusiness).append(DEFAULT_SEPARATOR)
                .append(life_videoStartFailureErrorsTech).append(DEFAULT_SEPARATOR)
                .append(life_exitDuringPreRoll).append(DEFAULT_SEPARATOR)
                .append(life_waitTimePrerollExitMs).append(DEFAULT_SEPARATOR)
                .append(life_lastCDNGroupId).append(DEFAULT_SEPARATOR)
                .append(life_lastCDNEdgeServer).append(DEFAULT_SEPARATOR)
                .append(interval_startTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_resourceId).append(DEFAULT_SEPARATOR)
                .append(switch_cdn).append(DEFAULT_SEPARATOR)
                .append(switch_justJoined).append(DEFAULT_SEPARATOR)
                .append(switch_hasJoined).append(DEFAULT_SEPARATOR)
                .append(switch_justJoinedAndLifeJoinTimeMsIsAccurate).append(DEFAULT_SEPARATOR)
                .append(switch_isEndedPlay).append(DEFAULT_SEPARATOR)
                .append(switch_isEnded).append(DEFAULT_SEPARATOR)
                .append(switch_isEndedPlayAndLifeAverageBitrateKbpsGT0).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoStartFailure).append(DEFAULT_SEPARATOR)
                .append(switch_videoStartFailureErrors).append(DEFAULT_SEPARATOR)
                .append(switch_isExitBeforeVideoStart).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoPlaybackFailure).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoStartSave).append(DEFAULT_SEPARATOR)
                .append(switch_videoPlaybackFailureErrors).append(DEFAULT_SEPARATOR)
                .append(switch_isAttempt).append(DEFAULT_SEPARATOR)
                .append(switch_playingTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_rebufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_networkRebufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_rebufferingDuringAdsMs).append(DEFAULT_SEPARATOR)
                .append(switch_adRelatedBufferingMs).append(DEFAULT_SEPARATOR)
                .append(switch_bitrateBytes).append(DEFAULT_SEPARATOR)
                .append(switch_bitrateTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_framesLoaded).append(DEFAULT_SEPARATOR)
                .append(switch_framesPlayingTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_seekJoinTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_seekJoinCount).append(DEFAULT_SEPARATOR)
                .append(switch_pcpBuckets1Min).append(DEFAULT_SEPARATOR)
                .append(switch_pcpIntervals).append(DEFAULT_SEPARATOR)
                .append(switch_rebufferingTimeMsRaw).append(DEFAULT_SEPARATOR)
                .append(switch_networkRebufferingTimeMsRaw).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoPlaybackFailureBusiness).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoPlaybackFailureTech).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoStartFailureBusiness).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoStartFailureTech).append(DEFAULT_SEPARATOR)
                .append(switch_videoPlaybackFailureErrorsBusiness).append(DEFAULT_SEPARATOR)
                .append(switch_videoPlaybackFailureErrorsTech).append(DEFAULT_SEPARATOR)
                .append(switch_videoStartFailureErrorsBusiness).append(DEFAULT_SEPARATOR)
                .append(switch_videoStartFailureErrorsTech).append(DEFAULT_SEPARATOR)
                .append(switch_adRequested).append(DEFAULT_SEPARATOR)
                .append(bucket_sessionTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_joinTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_playingTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_bufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_networkBufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_rebufferingRatioPct).append(DEFAULT_SEPARATOR)
                .append(bucket_networkRebufferingRatioPct).append(DEFAULT_SEPARATOR)
                .append(bucket_averageBitrateKbps).append(DEFAULT_SEPARATOR)
                .append(bucket_seekJoinTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_averageFrameRate).append(DEFAULT_SEPARATOR)
                .append(bucket_contentWatchedPct).append(DEFAULT_SEPARATOR)
                .append(tags.toString().replaceAll("=", ":"))
                .append(DEFAULT_LINE_END);

        return sb.toString();
    }

    public String toSparkTSV() {
        StringBuilder sb = new StringBuilder();
        sb.append(version).append(DEFAULT_SEPARATOR)
                .append(customerId).append(DEFAULT_SEPARATOR)
                .append(clientId).append(DEFAULT_SEPARATOR)
                .append(sessionId).append(DEFAULT_SEPARATOR)
                .append(segmentId).append(DEFAULT_SEPARATOR)
                .append(datasourceId).append(DEFAULT_SEPARATOR)
                .append(m3_dv_hwt).append(DEFAULT_SEPARATOR)
                .append(m3_dv_mrk).append(DEFAULT_SEPARATOR)
                .append(isAudienceOnly).append(DEFAULT_SEPARATOR)
                .append(isAd).append(DEFAULT_SEPARATOR)
                .append(assetName).append(DEFAULT_SEPARATOR)
                .append(streamUrl).append(DEFAULT_SEPARATOR)
                .append(contentLengthMs).append(DEFAULT_SEPARATOR)
                .append(ipType).append(DEFAULT_SEPARATOR)
                .append(geo_continent).append(DEFAULT_SEPARATOR)
                .append(geo_country).append(DEFAULT_SEPARATOR)
                .append(geo_state).append(DEFAULT_SEPARATOR)
                .append(geo_city).append(DEFAULT_SEPARATOR)
                .append(geo_dma).append(DEFAULT_SEPARATOR)
                .append(geo_asn).append(DEFAULT_SEPARATOR)
                .append(geo_isp).append(DEFAULT_SEPARATOR)
                .append(geo_postalCode).append(DEFAULT_SEPARATOR)
                .append(life_firstReceivedTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_latestReceivedTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_sessionTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_joinTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_playingTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_bufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_networkBufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_rebufferingRatioPct).append(DEFAULT_SEPARATOR)
                .append(life_networkRebufferingRatioPct).append(DEFAULT_SEPARATOR)
                .append(life_averageBitrateKbps).append(DEFAULT_SEPARATOR)
                .append(life_seekJoinTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_seekJoinCount).append(DEFAULT_SEPARATOR)
                .append(life_bufferingEvents).append(DEFAULT_SEPARATOR)
                .append(life_networkRebufferingEvents).append(DEFAULT_SEPARATOR)
                .append(life_bitrateKbps).append(DEFAULT_SEPARATOR)
                .append(life_contentWatchedTimeMs).append(DEFAULT_SEPARATOR)
                .append(life_contentWatchedPct).append(DEFAULT_SEPARATOR)
                .append(life_averageFrameRate).append(DEFAULT_SEPARATOR)
                .append(life_renderingQuality).append(DEFAULT_SEPARATOR)
                .append(life_resourceIds).append(DEFAULT_SEPARATOR)
                .append(life_cdns).append(DEFAULT_SEPARATOR)
                .append(life_fatalErrorResourceIds).append(DEFAULT_SEPARATOR)
                .append(life_fatalErrorCdns).append(DEFAULT_SEPARATOR)
                .append(life_latestErrorResourceId).append(DEFAULT_SEPARATOR)
                .append(life_latestErrorCdn).append(DEFAULT_SEPARATOR)
                .append(life_joinResourceIds).append(DEFAULT_SEPARATOR)
                .append(life_joinCdns).append(DEFAULT_SEPARATOR)
                .append(life_lastJoinCdn).append(DEFAULT_SEPARATOR)
                .append(life_lastCdn).append(DEFAULT_SEPARATOR)
                .append(life_lastJoinResourceId).append(DEFAULT_SEPARATOR)
                .append(life_isVideoPlaybackFailure).append(DEFAULT_SEPARATOR)
                .append(life_isVideoStartFailure).append(DEFAULT_SEPARATOR)
                .append(life_hasJoined).append(DEFAULT_SEPARATOR)
                .append(life_isVideoPlaybackFailureBusiness).append(DEFAULT_SEPARATOR)
                .append(life_isVideoPlaybackFailureTech).append(DEFAULT_SEPARATOR)
                .append(life_isVideoStartFailureBusiness).append(DEFAULT_SEPARATOR)
                .append(life_isVideoStartFailureTech).append(DEFAULT_SEPARATOR)
                .append(life_videoPlaybackFailureErrorsBusiness).append(DEFAULT_SEPARATOR)
                .append(life_videoPlaybackFailureErrorsTech).append(DEFAULT_SEPARATOR)
                .append(life_videoStartFailureErrorsBusiness).append(DEFAULT_SEPARATOR)
                .append(life_videoStartFailureErrorsTech).append(DEFAULT_SEPARATOR)
                .append(life_exitDuringPreRoll).append(DEFAULT_SEPARATOR)
                .append(life_waitTimePrerollExitMs).append(DEFAULT_SEPARATOR)
                .append(life_lastCDNGroupId).append(DEFAULT_SEPARATOR)
                .append(life_lastCDNEdgeServer).append(DEFAULT_SEPARATOR)
                .append(interval_startTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_resourceId).append(DEFAULT_SEPARATOR)
                .append(switch_cdn).append(DEFAULT_SEPARATOR)
                .append(switch_justJoined).append(DEFAULT_SEPARATOR)
                .append(switch_hasJoined).append(DEFAULT_SEPARATOR)
                .append(switch_justJoinedAndLifeJoinTimeMsIsAccurate).append(DEFAULT_SEPARATOR)
                .append(switch_isEndedPlay).append(DEFAULT_SEPARATOR)
                .append(switch_isEnded).append(DEFAULT_SEPARATOR)
                .append(switch_isEndedPlayAndLifeAverageBitrateKbpsGT0).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoStartFailure).append(DEFAULT_SEPARATOR)
                .append(switch_videoStartFailureErrors).append(DEFAULT_SEPARATOR)
                .append(switch_isExitBeforeVideoStart).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoPlaybackFailure).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoStartSave).append(DEFAULT_SEPARATOR)
                .append(switch_videoPlaybackFailureErrors).append(DEFAULT_SEPARATOR)
                .append(switch_isAttempt).append(DEFAULT_SEPARATOR)
                .append(switch_playingTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_rebufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_networkRebufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_rebufferingDuringAdsMs).append(DEFAULT_SEPARATOR)
                .append(switch_adRelatedBufferingMs).append(DEFAULT_SEPARATOR)
                .append(switch_bitrateBytes).append(DEFAULT_SEPARATOR)
                .append(switch_bitrateTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_framesLoaded).append(DEFAULT_SEPARATOR)
                .append(switch_framesPlayingTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_seekJoinTimeMs).append(DEFAULT_SEPARATOR)
                .append(switch_seekJoinCount).append(DEFAULT_SEPARATOR)
                .append(switch_pcpBuckets1Min).append(DEFAULT_SEPARATOR)
                .append(switch_pcpIntervals).append(DEFAULT_SEPARATOR)
                .append(switch_rebufferingTimeMsRaw).append(DEFAULT_SEPARATOR)
                .append(switch_networkRebufferingTimeMsRaw).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoPlaybackFailureBusiness).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoPlaybackFailureTech).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoStartFailureBusiness).append(DEFAULT_SEPARATOR)
                .append(switch_isVideoStartFailureTech).append(DEFAULT_SEPARATOR)
                .append(switch_videoPlaybackFailureErrorsBusiness).append(DEFAULT_SEPARATOR)
                .append(switch_videoPlaybackFailureErrorsTech).append(DEFAULT_SEPARATOR)
                .append(switch_videoStartFailureErrorsBusiness).append(DEFAULT_SEPARATOR)
                .append(switch_videoStartFailureErrorsTech).append(DEFAULT_SEPARATOR)
                .append(switch_adRequested).append(DEFAULT_SEPARATOR)
                .append(bucket_sessionTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_joinTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_playingTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_bufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_networkBufferingTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_rebufferingRatioPct).append(DEFAULT_SEPARATOR)
                .append(bucket_networkRebufferingRatioPct).append(DEFAULT_SEPARATOR)
                .append(bucket_averageBitrateKbps).append(DEFAULT_SEPARATOR)
                .append(bucket_seekJoinTimeMs).append(DEFAULT_SEPARATOR)
                .append(bucket_averageFrameRate).append(DEFAULT_SEPARATOR)
                .append(bucket_contentWatchedPct).append(DEFAULT_SEPARATOR)
                .append(c3_video_isAd).append(DEFAULT_SEPARATOR)
                .append(c3_ad_id).append(DEFAULT_SEPARATOR)
                .append(c3_ad_position).append(DEFAULT_SEPARATOR)
                .append(c3_ad_system).append(DEFAULT_SEPARATOR)
                .append(c3_ad_technology).append(DEFAULT_SEPARATOR)
                .append(c3_ad_isSlate).append(DEFAULT_SEPARATOR)
                .append(c3_ad_adStitcher).append(DEFAULT_SEPARATOR)
                .append(c3_ad_creativeId).append(DEFAULT_SEPARATOR)
                .append(c3_ad_breakId).append(DEFAULT_SEPARATOR)
                .append(c3_ad_contentAssetName).append(DEFAULT_SEPARATOR)
                .append(c3_pt_ver).append(DEFAULT_SEPARATOR)
                .append(c3_device_ua).append(DEFAULT_SEPARATOR)
                .append(c3_adaptor_type).append(DEFAULT_SEPARATOR)
                .append(c3_protocol_level).append(DEFAULT_SEPARATOR)
                .append(c3_protocol_pure).append(DEFAULT_SEPARATOR)
                .append(c3_pt_os_ver).append(DEFAULT_SEPARATOR)
                .append(c3_device_manufacturer).append(DEFAULT_SEPARATOR)
                .append(c3_device_brand).append(DEFAULT_SEPARATOR)
                .append(c3_device_model).append(DEFAULT_SEPARATOR)
                .append(c3_device_conn).append(DEFAULT_SEPARATOR)
                .append(c3_device_ver).append(DEFAULT_SEPARATOR)
                .append(c3_go_algoid).append(DEFAULT_SEPARATOR)
                .append(c3_player_name).append(DEFAULT_SEPARATOR)
                .append(c3_de_rs_raw).append(DEFAULT_SEPARATOR)
                .append(c3_de_bitr).append(DEFAULT_SEPARATOR)
                .append(c3_de_rsid).append(DEFAULT_SEPARATOR)
                .append(c3_de_cdn).append(DEFAULT_SEPARATOR)
                .append(c3_de_rs).append(DEFAULT_SEPARATOR)
                .append(c3_device_cver).append(DEFAULT_SEPARATOR)
                .append(c3_device_cver_bld).append(DEFAULT_SEPARATOR)
                .append(c3_video_isLive).append(DEFAULT_SEPARATOR)
                .append(c3_viewer_id).append(DEFAULT_SEPARATOR)
                .append(c3_client_hwType).append(DEFAULT_SEPARATOR)
                .append(c3_client_osname).append(DEFAULT_SEPARATOR)
                .append(c3_client_manufacturer).append(DEFAULT_SEPARATOR)
                .append(c3_client_brand).append(DEFAULT_SEPARATOR)
                .append(c3_client_marketingName).append(DEFAULT_SEPARATOR)
                .append(c3_client_model).append(DEFAULT_SEPARATOR)
                .append(c3_client_osv).append(DEFAULT_SEPARATOR)
                .append(c3_client_osf).append(DEFAULT_SEPARATOR)
                .append(c3_client_br).append(DEFAULT_SEPARATOR)
                .append(c3_client_brv).append(DEFAULT_SEPARATOR)
                .append(m3_dv_mnf).append(DEFAULT_SEPARATOR)
                .append(m3_dv_n).append(DEFAULT_SEPARATOR)
                .append(m3_dv_os).append(DEFAULT_SEPARATOR)
                .append(m3_dv_osv).append(DEFAULT_SEPARATOR)
                .append(m3_dv_osf).append(DEFAULT_SEPARATOR)
                .append(m3_dv_br).append(DEFAULT_SEPARATOR)
                .append(m3_dv_brv).append(DEFAULT_SEPARATOR)
                .append(m3_dv_fw).append(DEFAULT_SEPARATOR)
                .append(m3_dv_fwv).append(DEFAULT_SEPARATOR)
                .append(m3_dv_mod).append(DEFAULT_SEPARATOR)
                .append(m3_dv_vnd).append(DEFAULT_SEPARATOR)
                .append(m3_net_t).append(DEFAULT_SEPARATOR)
                .append(c3_protocol_type).append(DEFAULT_SEPARATOR)
                .append(c3_device_type).append(DEFAULT_SEPARATOR)
                .append(c3_pt_os).append(DEFAULT_SEPARATOR)
                .append(c3_ft_os).append(DEFAULT_SEPARATOR)
                .append(c3_framework).append(DEFAULT_SEPARATOR)
                .append(c3_framework_ver).append(DEFAULT_SEPARATOR)
                .append(c3_pt_br).append(DEFAULT_SEPARATOR)
                .append(c3_pt_br_ver).append(DEFAULT_SEPARATOR)
                .append(c3_br_v).append(DEFAULT_SEPARATOR)
                .append(appVersion).append(DEFAULT_SEPARATOR)
                .append(network).append(DEFAULT_SEPARATOR)
                .append(stormflowId).append(DEFAULT_SEPARATOR)
                .append(contentType).append(DEFAULT_SEPARATOR)
                .append(m3_dv_cat).append(DEFAULT_SEPARATOR)
                .append(c3_cp_an).append(DEFAULT_SEPARATOR)
                .append(huluPlayerFrameworkName).append(DEFAULT_SEPARATOR)
                .append(liveSignalProvider).append(DEFAULT_SEPARATOR)
                .append(clientFeatureTags).append(DEFAULT_SEPARATOR)
                .append(channel).append(DEFAULT_SEPARATOR)
                .append(huluPlayerFrameworkVersion).append(DEFAULT_SEPARATOR)
                .append(startType).append(DEFAULT_SEPARATOR)
                .append(plt).append(DEFAULT_SEPARATOR)
                .append(DevOps).append(DEFAULT_SEPARATOR)
                .append(conid).append(DEFAULT_SEPARATOR)
                .append(stormflow).append(DEFAULT_SEPARATOR)
                .append(tags.toString().replaceAll("=", ":"));

        return sb.toString();
    }
}
