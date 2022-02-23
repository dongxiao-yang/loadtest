package com.conviva.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ContentSessionToTSVMap {

    private static final Logger logger = Logger
            .getLogger(ContentSessionToTSVMap.class);

    public static final char DEFAULT_SEPARATOR = '\t';
    public static final String DEFAULT_LINE_END = "\n";
    public static final String quote = "'";

    static String buildStringArray(String str)
    {
        return str.toString().replaceAll(quote,"").replaceAll("\"","\'");
    }

    public static void main(String[] args) {

        String jsonfilename = "/Users/dyang/Documents/workspace/loadtest/loadtest/contentsession/part-00049-261e47f4-9870-46ff-a653-9c3eb284a606.c000.txt";
//        String jsonfilename = "/Users/dyang/Documents/workspace/loadtest/loadtest/ctsession";

//        ObjectMapper objectMapper = new ObjectMapper();
        try {
            LineNumberReader lineReader = new LineNumberReader(new FileReader(jsonfilename));

            String lineText = null;

            StringBuilder sb = new StringBuilder();
            sb.append("version").append(DEFAULT_SEPARATOR)
                    .append("customerId").append(DEFAULT_SEPARATOR)
                    .append("clientId").append(DEFAULT_SEPARATOR)
                    .append("sessionId").append(DEFAULT_SEPARATOR)
                    .append("segmentId").append(DEFAULT_SEPARATOR)
                    .append("datasourceId").append(DEFAULT_SEPARATOR)
                    .append("isAudienceOnly").append(DEFAULT_SEPARATOR)
                    .append("isAd").append(DEFAULT_SEPARATOR)
                    .append("assetName").append(DEFAULT_SEPARATOR)
                    .append("streamUrl").append(DEFAULT_SEPARATOR)
                    .append("contentLengthMs").append(DEFAULT_SEPARATOR)
                    .append("ipType").append(DEFAULT_SEPARATOR)
                    .append("geo_continent").append(DEFAULT_SEPARATOR)
                    .append("geo_country").append(DEFAULT_SEPARATOR)
                    .append("geo_state").append(DEFAULT_SEPARATOR)
                    .append("geo_city").append(DEFAULT_SEPARATOR)
                    .append("geo_dma").append(DEFAULT_SEPARATOR)
                    .append("geo_asn").append(DEFAULT_SEPARATOR)
                    .append("geo_isp").append(DEFAULT_SEPARATOR)
                    .append("geo_postalCode").append(DEFAULT_SEPARATOR)
                    .append("life_firstReceivedTimeMs").append(DEFAULT_SEPARATOR)
                    .append("life_latestReceivedTimeMs").append(DEFAULT_SEPARATOR)
                    .append("life_sessionTimeMs").append(DEFAULT_SEPARATOR)
                    .append("life_joinTimeMs").append(DEFAULT_SEPARATOR)
                    .append("life_playingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("life_bufferingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("life_networkBufferingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("life_rebufferingRatioPct").append(DEFAULT_SEPARATOR)
                    .append("life_networkRebufferingRatioPct").append(DEFAULT_SEPARATOR)
                    .append("life_averageBitrateKbps").append(DEFAULT_SEPARATOR)
                    .append("life_seekJoinTimeMs").append(DEFAULT_SEPARATOR)
                    .append("life_seekJoinCount").append(DEFAULT_SEPARATOR)
                    .append("life_bufferingEvents").append(DEFAULT_SEPARATOR)
                    .append("life_networkRebufferingEvents").append(DEFAULT_SEPARATOR)
                    .append("life_bitrateKbps").append(DEFAULT_SEPARATOR)
                    .append("life_contentWatchedTimeMs").append(DEFAULT_SEPARATOR)
                    .append("life_contentWatchedPct").append(DEFAULT_SEPARATOR)
                    .append("life_averageFrameRate").append(DEFAULT_SEPARATOR)
                    .append("life_renderingQuality").append(DEFAULT_SEPARATOR)
                    .append("life_resourceIds").append(DEFAULT_SEPARATOR)
                    .append("life_cdns").append(DEFAULT_SEPARATOR)
                    .append("life_fatalErrorResourceIds").append(DEFAULT_SEPARATOR)
                    .append("life_fatalErrorCdns").append(DEFAULT_SEPARATOR)
                    .append("life_latestErrorResourceId").append(DEFAULT_SEPARATOR)
                    .append("life_latestErrorCdn").append(DEFAULT_SEPARATOR)
                    .append("life_joinResourceIds").append(DEFAULT_SEPARATOR)
                    .append("life_joinCdns").append(DEFAULT_SEPARATOR)
                    .append("life_lastJoinCdn").append(DEFAULT_SEPARATOR)
                    .append("life_lastCdn").append(DEFAULT_SEPARATOR)
                    .append("life_lastJoinResourceId").append(DEFAULT_SEPARATOR)
                    .append("life_isVideoPlaybackFailure").append(DEFAULT_SEPARATOR)
                    .append("life_isVideoStartFailure").append(DEFAULT_SEPARATOR)
                    .append("life_hasJoined").append(DEFAULT_SEPARATOR)
                    .append("life_isVideoPlaybackFailureBusiness").append(DEFAULT_SEPARATOR)
                    .append("life_isVideoPlaybackFailureTech").append(DEFAULT_SEPARATOR)
                    .append("life_isVideoStartFailureBusiness").append(DEFAULT_SEPARATOR)
                    .append("life_isVideoStartFailureTech").append(DEFAULT_SEPARATOR)
                    .append("life_videoPlaybackFailureErrorsBusiness").append(DEFAULT_SEPARATOR)
                    .append("life_videoPlaybackFailureErrorsTech").append(DEFAULT_SEPARATOR)
                    .append("life_videoStartFailureErrorsBusiness").append(DEFAULT_SEPARATOR)
                    .append("life_videoStartFailureErrorsTech").append(DEFAULT_SEPARATOR)
                    .append("life_exitDuringPreRoll").append(DEFAULT_SEPARATOR)
                    .append("life_waitTimePrerollExitMs").append(DEFAULT_SEPARATOR)
                    .append("life_lastCDNGroupId").append(DEFAULT_SEPARATOR)
                    .append("life_lastCDNEdgeServer").append(DEFAULT_SEPARATOR)
                    .append("interval_startTimeMs").append(DEFAULT_SEPARATOR)
                    .append("switch_resourceId").append(DEFAULT_SEPARATOR)
                    .append("switch_cdn").append(DEFAULT_SEPARATOR)
                    .append("switch_justJoined").append(DEFAULT_SEPARATOR)
                    .append("switch_hasJoined").append(DEFAULT_SEPARATOR)
                    .append("switch_justJoinedAndLifeJoinTimeMsIsAccurate").append(DEFAULT_SEPARATOR)
                    .append("switch_isEndedPlay").append(DEFAULT_SEPARATOR)
                    .append("switch_isEnded").append(DEFAULT_SEPARATOR)
                    .append("switch_isEndedPlayAndLifeAverageBitrateKbpsGT0").append(DEFAULT_SEPARATOR)
                    .append("switch_isVideoStartFailure").append(DEFAULT_SEPARATOR)
                    .append("switch_videoStartFailureErrors").append(DEFAULT_SEPARATOR)
                    .append("switch_isExitBeforeVideoStart").append(DEFAULT_SEPARATOR)
                    .append("switch_isVideoPlaybackFailure").append(DEFAULT_SEPARATOR)
                    .append("switch_isVideoStartSave").append(DEFAULT_SEPARATOR)
                    .append("switch_videoPlaybackFailureErrors").append(DEFAULT_SEPARATOR)
                    .append("switch_isAttempt").append(DEFAULT_SEPARATOR)
                    .append("switch_playingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("switch_rebufferingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("switch_networkRebufferingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("switch_rebufferingDuringAdsMs").append(DEFAULT_SEPARATOR)
                    .append("switch_adRelatedBufferingMs").append(DEFAULT_SEPARATOR)
                    .append("switch_bitrateBytes").append(DEFAULT_SEPARATOR)
                    .append("switch_bitrateTimeMs").append(DEFAULT_SEPARATOR)
                    .append("switch_framesLoaded").append(DEFAULT_SEPARATOR)
                    .append("switch_framesPlayingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("switch_seekJoinTimeMs").append(DEFAULT_SEPARATOR)
                    .append("switch_seekJoinCount").append(DEFAULT_SEPARATOR)
                    .append("switch_pcpBuckets1Min").append(DEFAULT_SEPARATOR)
                    .append("switch_pcpIntervals").append(DEFAULT_SEPARATOR)
                    .append("switch_rebufferingTimeMsRaw").append(DEFAULT_SEPARATOR)
                    .append("switch_networkRebufferingTimeMsRaw").append(DEFAULT_SEPARATOR)
                    .append("switch_isVideoPlaybackFailureBusiness").append(DEFAULT_SEPARATOR)
                    .append("switch_isVideoPlaybackFailureTech").append(DEFAULT_SEPARATOR)
                    .append("switch_isVideoStartFailureBusiness").append(DEFAULT_SEPARATOR)
                    .append("switch_isVideoStartFailureTech").append(DEFAULT_SEPARATOR)
                    .append("switch_videoPlaybackFailureErrorsBusiness").append(DEFAULT_SEPARATOR)
                    .append("switch_videoPlaybackFailureErrorsTech").append(DEFAULT_SEPARATOR)
                    .append("switch_videoStartFailureErrorsBusiness").append(DEFAULT_SEPARATOR)
                    .append("switch_videoStartFailureErrorsTech").append(DEFAULT_SEPARATOR)
                    .append("switch_adRequested").append(DEFAULT_SEPARATOR)
                    .append("bucket_sessionTimeMs").append(DEFAULT_SEPARATOR)
                    .append("bucket_joinTimeMs").append(DEFAULT_SEPARATOR)
                    .append("bucket_playingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("bucket_bufferingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("bucket_networkBufferingTimeMs").append(DEFAULT_SEPARATOR)
                    .append("bucket_rebufferingRatioPct").append(DEFAULT_SEPARATOR)
                    .append("bucket_networkRebufferingRatioPct").append(DEFAULT_SEPARATOR)
                    .append("bucket_averageBitrateKbps").append(DEFAULT_SEPARATOR)
                    .append("bucket_seekJoinTimeMs").append(DEFAULT_SEPARATOR)
                    .append("bucket_averageFrameRate").append(DEFAULT_SEPARATOR)
                    .append("bucket_contentWatchedPct").append(DEFAULT_SEPARATOR)
                    .append("tags")
                    .append(DEFAULT_LINE_END);

            String head = sb.toString();


            PrintWriter writer = new PrintWriter(new File("contentSession.tsv"));

            writer.write(head);

            while ((lineText = lineReader.readLine()) != null) {

                JsonNode jsonTree = new ObjectMapper().readTree(lineText);
                CsvMapper csvMapper = new CsvMapper();

                ContentSession cs = new ContentSession();


                Map<String, String> tags = new HashMap<>();

                for (Iterator<Map.Entry<String, JsonNode>> it = jsonTree.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> jn = it.next();

                    String nodeName = jn.getKey();
                    String valueStr = jn.getValue().asText();
//                    String valueStr = jn.getValue().asText().replaceAll(quote,"");


                    if ("version".equals(nodeName)) {
                        cs.setVersion(valueStr);
                    } else if ("customerId".equals(nodeName)) {
                        cs.setCustomerId(Integer.parseInt(valueStr));
                    } else if ("clientId".equals(nodeName)) {
                        cs.setClientId(valueStr);
                    } else if
                    ("sessionId".equals(nodeName)) {
                        cs.setSessionId(Integer.parseInt(valueStr));
                    } else if
                    ("segmentId".equals(nodeName)) {
                        cs.setSegmentId(Integer.parseInt(valueStr));
                    } else if
                    ("datasourceId".equals(nodeName)) {
                        cs.setDatasourceId(valueStr);
                    } else if
                    ("isAudienceOnly".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setIsAudienceOnly(i);
                    } else if
                    ("isAd".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setIsAd(i);
                    } else if
                    ("assetName".equals(nodeName)) {
                        cs.setAssetName(valueStr.replaceAll(DEFAULT_LINE_END,""));
                    } else if
                    ("streamUrl".equals(nodeName)) {
                        cs.setStreamUrl(valueStr);
                    } else if
                    ("contentLengthMs".equals(nodeName)) {
                        cs.setContentLengthMs(Integer.parseInt(valueStr));
                    } else if
                    ("ipType".equals(nodeName)) {
                        cs.setIpType(valueStr);
                    } else if
                    ("geo.continent".equals(nodeName)) {
                        cs.setGeo_continent(Long.parseLong(valueStr));
                    } else if
                    ("geo.country".equals(nodeName)) {
                        cs.setGeo_country(Long.parseLong(valueStr));
                    } else if
                    ("geo.state".equals(nodeName)) {
                        cs.setGeo_state(Long.parseLong(valueStr));
                    } else if
                    ("geo.city".equals(nodeName)) {
                        cs.setGeo_city(Long.parseLong(valueStr));
                    } else if
                    ("geo.dma".equals(nodeName)) {
                        cs.setGeo_dma(Integer.parseInt(valueStr));
                    } else if
                    ("geo.asn".equals(nodeName)) {
                        cs.setGeo_asn(Integer.parseInt(valueStr));
                    } else if
                    ("geo.isp".equals(nodeName)) {
                        cs.setGeo_isp(Integer.parseInt(valueStr));
                    } else if
                    ("geo.postalCode".equals(nodeName)) {
                        cs.setGeo_postalCode(valueStr);
                    } else if
                    ("life.firstReceivedTimeMs".equals(nodeName)) {
                        cs.setLife_firstReceivedTimeMs(Long.parseLong(valueStr));
                    } else if
                    ("life.latestReceivedTimeMs".equals(nodeName)) {
                        cs.setLife_latestReceivedTimeMs(Long.parseLong(valueStr));
                    } else if
                    ("life.sessionTimeMs".equals(nodeName)) {
                        cs.setLife_sessionTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life.joinTimeMs".equals(nodeName)) {
                        cs.setLife_joinTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life.playingTimeMs".equals(nodeName)) {
                        cs.setLife_playingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life.bufferingTimeMs".equals(nodeName)) {
                        cs.setLife_bufferingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life.networkBufferingTimeMs".equals(nodeName)) {
                        cs.setLife_networkBufferingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life.rebufferingRatioPct".equals(nodeName)) {
                        cs.setLife_rebufferingRatioPct(Float.parseFloat(valueStr));
                    } else if
                    ("life.networkRebufferingRatioPct".equals(nodeName)) {
                        cs.setLife_networkRebufferingRatioPct(Float.parseFloat(valueStr));
                    } else if
                    ("life.averageBitrateKbps".equals(nodeName)) {
                        cs.setLife_averageBitrateKbps(Integer.parseInt(valueStr));
                    } else if
                    ("life.seekJoinTimeMs".equals(nodeName)) {
                        cs.setLife_seekJoinTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life.seekJoinCount".equals(nodeName)) {
                        cs.setLife_seekJoinCount(Integer.parseInt(valueStr));
                    } else if
                    ("life.bufferingEvents".equals(nodeName)) {
                        cs.setLife_bufferingEvents(Integer.parseInt(valueStr));
                    } else if
                    ("life.networkRebufferingEvents".equals(nodeName)) {
                        cs.setLife_networkRebufferingEvents(Integer.parseInt(valueStr));
                    } else if
                    ("life.bitrateKbps".equals(nodeName)) {
                        cs.setLife_bitrateKbps(Integer.parseInt(valueStr));
                    } else if
                    ("life.contentWatchedTimeMs".equals(nodeName)) {
                        cs.setLife_contentWatchedTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life.contentWatchedPct".equals(nodeName)) {
                        cs.setLife_contentWatchedPct(Float.parseFloat(valueStr));
                    } else if
                    ("life.averageFrameRate".equals(nodeName)) {
                        cs.setLife_averageFrameRate(Integer.parseInt(valueStr));
                    } else if
                    ("life.renderingQuality".equals(nodeName)) {
                        cs.setLife_renderingQuality(Integer.parseInt(valueStr));
                    } else if
                    ("life.resourceIds".equals(nodeName)) {
                        cs.setLife_resourceIds(jn.getValue().toString());
                    } else if
                    ("life.cdns".equals(nodeName)) {
                        cs.setLife_cdns(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("life.fatalErrorResourceIds".equals(nodeName)) {
                        cs.setLife_fatalErrorResourceIds(jn.getValue().toString());
                    } else if
                    ("life.fatalErrorCdns".equals(nodeName)) {
                        cs.setLife_fatalErrorCdns(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("life.latestErrorResourceId".equals(nodeName)) {
                        cs.setLife_latestErrorResourceId(Integer.parseInt(valueStr));
                    } else if
                    ("life.latestErrorCdn".equals(nodeName)) {
                        cs.setLife_latestErrorCdn(valueStr);
                    } else if
                    ("life.joinResourceIds".equals(nodeName)) {
                        String aaa = jn.getValue().toString();
                        cs.setLife_joinResourceIds(jn.getValue().toString());
                    } else if
                    ("life.joinCdns".equals(nodeName)) {
                        cs.setLife_joinCdns(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("life.lastJoinCdn".equals(nodeName)) {
                        cs.setLife_lastJoinCdn(valueStr);
                    } else if
                    ("life.lastCdn".equals(nodeName)) {
                        cs.setLife_lastCdn(valueStr);
                    } else if
                    ("life.lastJoinResourceId".equals(nodeName)) {
                        cs.setLife_lastJoinResourceId(Integer.parseInt(valueStr));
                    } else if
                    ("life.isVideoPlaybackFailure".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoPlaybackFailure(i);
                    } else if
                    ("life.isVideoStartFailure".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoStartFailure(i);
                    } else if
                    ("life.hasJoined".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_hasJoined(i);
                    } else if
                    ("life.isVideoPlaybackFailureBusiness".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoPlaybackFailureBusiness(i);
                    } else if
                    ("life.isVideoPlaybackFailureTech".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoPlaybackFailureTech(i);
                    } else if
                    ("life.isVideoStartFailureBusiness".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoStartFailureBusiness(i);
                    } else if
                    ("life.isVideoStartFailureTech".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoStartFailureTech(i);
                    } else if
                    ("life.videoPlaybackFailureErrorsBusiness".equals(nodeName)) {
                        cs.setLife_videoPlaybackFailureErrorsBusiness(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("life.videoPlaybackFailureErrorsTech".equals(nodeName)) {
                        cs.setLife_videoPlaybackFailureErrorsTech(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("life.videoStartFailureErrorsBusiness".equals(nodeName)) {
                        cs.setLife_videoStartFailureErrorsBusiness(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("life.videoStartFailureErrorsTech".equals(nodeName)) {
//                        if("[\"Can't load the video\"]".equals(jn.getValue().toString()))
//                        {
//                            String aaa = jn.getValue().toString().replaceAll(quote,"");
//
//                            String bbb = aaa.replaceAll("\"","\'");
//                            String ccc = buildStringArray(jn.getValue().toString());
//                            String ddd = ccc;
//                        }
                        cs.setLife_videoStartFailureErrorsTech(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("life.exitDuringPreRoll".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_exitDuringPreRoll(i);
                    } else if
                    ("life.waitTimePrerollExitMs".equals(nodeName)) {
                        cs.setLife_waitTimePrerollExitMs(Integer.parseInt(valueStr));
                    } else if
                    ("life.lastCDNGroupId".equals(nodeName)) {
                        cs.setLife_lastCDNGroupId(valueStr);
                    } else if
                    ("life.lastCDNEdgeServer".equals(nodeName)) {
                        cs.setLife_lastCDNEdgeServer(valueStr);
                    } else if
                    ("interval.startTimeMs".equals(nodeName)) {
                        cs.setInterval_startTimeMs(Long.parseLong(valueStr));
                    } else if
                    ("switch.resourceId".equals(nodeName)) {
                        cs.setSwitch_resourceId(Integer.parseInt(valueStr));
                    } else if
                    ("switch.cdn".equals(nodeName)) {
                        cs.setSwitch_cdn(valueStr);
                    } else if
                    ("switch.justJoined".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_justJoined(i);
                    } else if
                    ("switch.hasJoined".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_hasJoined(i);
                    } else if
                    ("switch.justJoinedAndLifeJoinTimeMsIsAccurate".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_justJoinedAndLifeJoinTimeMsIsAccurate(i);
                    } else if
                    ("switch.isEndedPlay".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isEndedPlay(i);
                    } else if
                    ("switch.isEnded".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isEnded(i);
                    } else if
                    ("switch.isEndedPlayAndLifeAverageBitrateKbpsGT0".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isEndedPlayAndLifeAverageBitrateKbpsGT0(i);
                    } else if
                    ("switch.isVideoStartFailure".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isVideoStartFailure(i);
                    } else if
                    ("switch.videoStartFailureErrors".equals(nodeName)) {
                        cs.setSwitch_videoStartFailureErrors(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("switch.isExitBeforeVideoStart".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isExitBeforeVideoStart(i);
                    } else if
                    ("switch.isVideoPlaybackFailure".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isVideoPlaybackFailure(i);
                    } else if
                    ("switch.isVideoStartSave".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isVideoStartSave(i);
                    } else if
                    ("switch.videoPlaybackFailureErrors".equals(nodeName)) {
                        cs.setSwitch_videoPlaybackFailureErrors(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("switch.isAttempt".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isAttempt(i);
                    } else if
                    ("switch.playingTimeMs".equals(nodeName)) {
                        cs.setSwitch_playingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("switch.rebufferingTimeMs".equals(nodeName)) {
                        cs.setSwitch_rebufferingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("switch.networkRebufferingTimeMs".equals(nodeName)) {
                        cs.setSwitch_networkRebufferingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("switch.rebufferingDuringAdsMs".equals(nodeName)) {
                        cs.setSwitch_rebufferingDuringAdsMs(Integer.parseInt(valueStr));
                    } else if
                    ("switch.adRelatedBufferingMs".equals(nodeName)) {
                        cs.setSwitch_adRelatedBufferingMs(Integer.parseInt(valueStr));
                    } else if
                    ("switch.bitrateBytes".equals(nodeName)) {
                        cs.setSwitch_bitrateBytes(Long.parseLong(valueStr));
                    } else if
                    ("switch.bitrateTimeMs".equals(nodeName)) {
                        cs.setSwitch_bitrateTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("switch.framesLoaded".equals(nodeName)) {
                        cs.setSwitch_framesLoaded(Integer.parseInt(valueStr));
                    } else if
                    ("switch.framesPlayingTimeMs".equals(nodeName)) {
                        cs.setSwitch_framesPlayingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("switch.seekJoinTimeMs".equals(nodeName)) {
                        cs.setSwitch_seekJoinTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("switch.seekJoinCount".equals(nodeName)) {
                        cs.setSwitch_seekJoinCount(Integer.parseInt(valueStr));
                    } else if
                    ("switch.pcpBuckets1Min".equals(nodeName)) {
                        cs.setSwitch_pcpBuckets1Min(jn.getValue().toString());
                    } else if
                    ("switch.pcpIntervals".equals(nodeName)) {
                        cs.setSwitch_pcpIntervals(Long.parseLong(valueStr));
                    } else if
                    ("switch.rebufferingTimeMsRaw".equals(nodeName)) {
                        cs.setSwitch_rebufferingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("switch.networkRebufferingTimeMsRaw".equals(nodeName)) {
                        cs.setSwitch_networkRebufferingTimeMsRaw(Integer.parseInt(valueStr));
                    } else if
                    ("switch.isVideoPlaybackFailureBusiness".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isVideoPlaybackFailureBusiness(i);
                    } else if
                    ("switch.isVideoPlaybackFailureTech".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoPlaybackFailureTech(i);
                    } else if
                    ("switch.isVideoStartFailureBusiness".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isVideoStartFailureBusiness(i);
                    } else if
                    ("switch.isVideoStartFailureTech".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_isVideoStartFailureTech(i);
                    } else if
                    ("switch.videoPlaybackFailureErrorsBusiness".equals(nodeName)) {
                        cs.setSwitch_videoPlaybackFailureErrorsBusiness(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("switch.videoPlaybackFailureErrorsTech".equals(nodeName)) {
                        cs.setSwitch_videoPlaybackFailureErrorsTech(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("switch.videoStartFailureErrorsBusiness".equals(nodeName)) {
                        cs.setSwitch_videoStartFailureErrorsBusiness(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("switch.videoStartFailureErrorsTech".equals(nodeName)) {
                        cs.setSwitch_videoStartFailureErrorsTech(buildStringArray(jn.getValue().toString()));
                    } else if
                    ("switch.adRequested".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setSwitch_adRequested(i);
                    } else if
                    ("bucket.sessionTimeMs".equals(nodeName)) {
                        cs.setBucket_sessionTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("bucket.joinTimeMs".equals(nodeName)) {
                        cs.setBucket_joinTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("bucket.playingTimeMs".equals(nodeName)) {
                        cs.setBucket_playingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("bucket.bufferingTimeMs".equals(nodeName)) {
                        cs.setBucket_bufferingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("bucket.networkBufferingTimeMs".equals(nodeName)) {
                        cs.setBucket_networkBufferingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("bucket.rebufferingRatioPct".equals(nodeName)) {
                        cs.setBucket_rebufferingRatioPct(Float.parseFloat(valueStr));
                    } else if
                    ("bucket.networkRebufferingRatioPct".equals(nodeName)) {
                        cs.setBucket_networkRebufferingRatioPct(Float.parseFloat(valueStr));
                    } else if
                    ("bucket.averageBitrateKbps".equals(nodeName)) {
                        cs.setBucket_averageBitrateKbps(Integer.parseInt(valueStr));
                    } else if
                    ("bucket.seekJoinTimeMs".equals(nodeName)) {
                        cs.setBucket_seekJoinTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("bucket.averageFrameRate".equals(nodeName)) {
                        cs.setBucket_averageFrameRate(Integer.parseInt(valueStr));
                    } else if
                    ("bucket.contentWatchedPct".equals(nodeName)) {
                        cs.setBucket_contentWatchedPct(Float.parseFloat(valueStr));
                    } else if (nodeName.startsWith("tags.")) {
                        String key = quote + nodeName.substring(5) + quote;
                        tags.put(key, (quote + valueStr.replaceAll(quote,"") + quote).replaceAll(DEFAULT_LINE_END,""));
                    } else {
//                        logger.warn(">>> unknown field:" + jn.toString());
                    }


                }

                cs.setTags(tags);
                writer.write(cs.toTSV());
//               break;

            }
            writer.flush();
            writer.close();


        } catch (IOException ex) {
            System.err.println(ex);
        }
    }


}
