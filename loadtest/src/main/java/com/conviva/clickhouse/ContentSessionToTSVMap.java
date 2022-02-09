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

    public static void main(String[] args) {

//        String jsonfilename = "/Users/dyang/part-00002-64edcea8-84f4-4264-8c2f-55bc2237fe56.c000.txt";
        String jsonfilename = "/Users/dyang/Documents/workspace/loadtest/loadtest/20220116_hour";

        ObjectMapper objectMapper = new ObjectMapper();
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


            PrintWriter writer = new PrintWriter(new File("PII_map_20220116.tsv"));

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
                        cs.setAssetName(valueStr);
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
                    ("geo_continent".equals(nodeName)) {
                        cs.setGeo_continent(Long.parseLong(valueStr));
                    } else if
                    ("geo_country".equals(nodeName)) {
                        cs.setGeo_country(Long.parseLong(valueStr));
                    } else if
                    ("geo_state".equals(nodeName)) {
                        cs.setGeo_state(Long.parseLong(valueStr));
                    } else if
                    ("geo_city".equals(nodeName)) {
                        cs.setGeo_city(Long.parseLong(valueStr));
                    } else if
                    ("geo_dma".equals(nodeName)) {
                        cs.setGeo_dma(Integer.parseInt(valueStr));
                    } else if
                    ("geo_asn".equals(nodeName)) {
                        cs.setGeo_asn(Integer.parseInt(valueStr));
                    } else if
                    ("geo_isp".equals(nodeName)) {
                        cs.setGeo_isp(Integer.parseInt(valueStr));
                    } else if
                    ("geo_postalCode".equals(nodeName)) {
                        cs.setGeo_postalCode(valueStr);
                    } else if
                    ("life_firstReceivedTimeMs".equals(nodeName)) {
                        cs.setLife_firstReceivedTimeMs(Long.parseLong(valueStr));
                    } else if
                    ("life_latestReceivedTimeMs".equals(nodeName)) {
                        cs.setLife_latestReceivedTimeMs(Long.parseLong(valueStr));
                    } else if
                    ("life_sessionTimeMs".equals(nodeName)) {
                        cs.setLife_sessionTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life_joinTimeMs".equals(nodeName)) {
                        cs.setLife_joinTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life_playingTimeMs".equals(nodeName)) {
                        cs.setLife_playingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life_bufferingTimeMs".equals(nodeName)) {
                        cs.setLife_bufferingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life_networkBufferingTimeMs".equals(nodeName)) {
                        cs.setLife_networkBufferingTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life_rebufferingRatioPct".equals(nodeName)) {
                        cs.setLife_rebufferingRatioPct(Float.parseFloat(valueStr));
                    } else if
                    ("life_networkRebufferingRatioPct".equals(nodeName)) {
                        cs.setLife_networkRebufferingRatioPct(Float.parseFloat(valueStr));
                    } else if
                    ("life_averageBitrateKbps".equals(nodeName)) {
                        cs.setLife_averageBitrateKbps(Integer.parseInt(valueStr));
                    } else if
                    ("life_seekJoinTimeMs".equals(nodeName)) {
                        cs.setLife_seekJoinTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life_seekJoinCount".equals(nodeName)) {
                        cs.setLife_seekJoinCount(Integer.parseInt(valueStr));
                    } else if
                    ("life_bufferingEvents".equals(nodeName)) {
                        cs.setLife_bufferingEvents(Integer.parseInt(valueStr));
                    } else if
                    ("life_networkRebufferingEvents".equals(nodeName)) {
                        cs.setLife_networkRebufferingEvents(Integer.parseInt(valueStr));
                    } else if
                    ("life_bitrateKbps".equals(nodeName)) {
                        cs.setLife_bitrateKbps(Integer.parseInt(valueStr));
                    } else if
                    ("life_contentWatchedTimeMs".equals(nodeName)) {
                        cs.setLife_contentWatchedTimeMs(Integer.parseInt(valueStr));
                    } else if
                    ("life_contentWatchedPct".equals(nodeName)) {
                        cs.setLife_contentWatchedPct(Float.parseFloat(valueStr));
                    } else if
                    ("life_averageFrameRate".equals(nodeName)) {
                        cs.setLife_averageFrameRate(Integer.parseInt(valueStr));
                    } else if
                    ("life_renderingQuality".equals(nodeName)) {
                        cs.setLife_renderingQuality(Integer.parseInt(valueStr));
                    } else if
                    ("life_resourceIds".equals(nodeName)) {
                        cs.setLife_resourceIds(valueStr);
                    } else if
                    ("life_cdns".equals(nodeName)) {
                        cs.setLife_cdns(valueStr);
                    } else if
                    ("life_fatalErrorResourceIds".equals(nodeName)) {
                        cs.setLife_fatalErrorResourceIds(valueStr);
                    } else if
                    ("life_fatalErrorCdns".equals(nodeName)) {
                        cs.setLife_fatalErrorCdns(valueStr);
                    } else if
                    ("life_latestErrorResourceId".equals(nodeName)) {
                        cs.setLife_latestErrorResourceId(valueStr);
                    } else if
                    ("life_latestErrorCdn".equals(nodeName)) {
                        cs.setLife_latestErrorCdn(valueStr);
                    } else if
                    ("life_joinResourceIds".equals(nodeName)) {
                        cs.setLife_joinResourceIds(valueStr);
                    } else if
                    ("life_joinCdns".equals(nodeName)) {
                        cs.setLife_joinCdns(valueStr);
                    } else if
                    ("life_lastJoinCdn".equals(nodeName)) {
                        cs.setLife_lastJoinCdn(valueStr);
                    } else if
                    ("life_lastCdn".equals(nodeName)) {
                        cs.setLife_lastCdn(valueStr);
                    } else if
                    ("life_lastJoinResourceId".equals(nodeName)) {
                        cs.setLife_lastJoinResourceId(Integer.parseInt(valueStr));
                    } else if
                    ("life_isVideoPlaybackFailure".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoPlaybackFailure(i);
                    } else if
                    ("life_isVideoStartFailure".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoStartFailure(i);
                    } else if
                    ("life_hasJoined".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_hasJoined(i);
                    } else if
                    ("life_isVideoPlaybackFailureBusiness".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoPlaybackFailureBusiness(i);
                    } else if
                    ("life_isVideoPlaybackFailureTech".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoPlaybackFailureTech(i);
                    } else if
                    ("life_isVideoStartFailureBusiness".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoStartFailureBusiness(i);
                    } else if
                    ("life_isVideoStartFailureTech".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_isVideoStartFailureTech(i);
                    } else if
                    ("life_videoPlaybackFailureErrorsBusiness".equals(nodeName)) {
                        cs.setLife_videoPlaybackFailureErrorsBusiness(valueStr);
                    } else if
                    ("life_videoPlaybackFailureErrorsTech".equals(nodeName)) {
                        cs.setLife_videoPlaybackFailureErrorsTech(valueStr);
                    } else if
                    ("life_videoStartFailureErrorsBusiness".equals(nodeName)) {
                        cs.setLife_videoStartFailureErrorsBusiness(valueStr);
                    } else if
                    ("life_videoStartFailureErrorsTech".equals(nodeName)) {
                        cs.setLife_videoStartFailureErrorsTech(valueStr);
                    } else if
                    ("life_exitDuringPreRoll".equals(nodeName)) {
                        int i = "true".equals(valueStr) ? 1 : 0;
                        cs.setLife_exitDuringPreRoll(i);
                    } else if
                    ("life_waitTimePrerollExitMs".equals(nodeName)) {
                        cs.setLife_waitTimePrerollExitMs(Integer.parseInt(valueStr));
                    } else if
                    ("life_lastCDNGroupId".equals(nodeName)) {
                        cs.setLife_lastCDNGroupId(valueStr);
                    } else if
                    ("life_lastCDNEdgeServer".equals(nodeName)) {
                        cs.setLife_lastCDNEdgeServer(valueStr);
                    } else if
                    ("interval_startTimeMs".equals(nodeName)) {
                        cs.setInterval_startTimeMs(Long.parseLong(valueStr));
                    } else if
                    ("switch_resourceId".equals(nodeName)) {
                    } else if
                    ("switch_cdn".equals(nodeName)) {
                    } else if
                    ("switch_justJoined".equals(nodeName)) {
                    } else if
                    ("switch_hasJoined".equals(nodeName)) {
                    } else if
                    ("switch_justJoinedAndLifeJoinTimeMsIsAccurate".equals(nodeName)) {
                    } else if
                    ("switch_isEndedPlay".equals(nodeName)) {
                    } else if
                    ("switch_isEnded".equals(nodeName)) {
                    } else if
                    ("switch_isEndedPlayAndLifeAverageBitrateKbpsGT0".equals(nodeName)) {
                    } else if
                    ("switch_isVideoStartFailure".equals(nodeName)) {
                    } else if
                    ("switch_videoStartFailureErrors".equals(nodeName)) {
                    } else if
                    ("switch_isExitBeforeVideoStart".equals(nodeName)) {
                    } else if
                    ("switch_isVideoPlaybackFailure".equals(nodeName)) {
                    } else if
                    ("switch_isVideoStartSave".equals(nodeName)) {
                    } else if
                    ("switch_videoPlaybackFailureErrors".equals(nodeName)) {
                    } else if
                    ("switch_isAttempt".equals(nodeName)) {
                    } else if
                    ("switch_playingTimeMs".equals(nodeName)) {
                    } else if
                    ("switch_rebufferingTimeMs".equals(nodeName)) {
                    } else if
                    ("switch_networkRebufferingTimeMs".equals(nodeName)) {
                    } else if
                    ("switch_rebufferingDuringAdsMs".equals(nodeName)) {
                    } else if
                    ("switch_adRelatedBufferingMs".equals(nodeName)) {
                    } else if
                    ("switch_bitrateBytes".equals(nodeName)) {
                    } else if
                    ("switch_bitrateTimeMs".equals(nodeName)) {
                    } else if
                    ("switch_framesLoaded".equals(nodeName)) {
                    } else if
                    ("switch_framesPlayingTimeMs".equals(nodeName)) {
                    } else if
                    ("switch_seekJoinTimeMs".equals(nodeName)) {
                    } else if
                    ("switch_seekJoinCount".equals(nodeName)) {
                    } else if
                    ("switch_isVideoPlaybackFailureBusiness".equals(nodeName)) {
                    } else if
                    ("switch_isVideoPlaybackFailureTech".equals(nodeName)) {
                    } else if
                    ("switch_isVideoStartFailureBusiness".equals(nodeName)) {
                    } else if
                    ("switch_isVideoStartFailureTech".equals(nodeName)) {
                    } else if
                    ("switch_videoPlaybackFailureErrorsBusiness".equals(nodeName)) {
                    } else if
                    ("switch_videoPlaybackFailureErrorsTech".equals(nodeName)) {
                    } else if
                    ("switch_videoStartFailureErrorsBusiness".equals(nodeName)) {
                    } else if
                    ("switch_videoStartFailureErrorsTech".equals(nodeName)) {
                    } else if
                    ("switch_adRequested".equals(nodeName)) {
                    } else if
                    ("bucket_sessionTimeMs".equals(nodeName)) {
                    } else if
                    ("bucket_playingTimeMs".equals(nodeName)) {
                    } else if
                    ("bucket_bufferingTimeMs".equals(nodeName)) {
                    } else if
                    ("bucket_networkBufferingTimeMs".equals(nodeName)) {
                    } else if
                    ("bucket_rebufferingRatioPct".equals(nodeName)) {
                    } else if
                    ("bucket_networkRebufferingRatioPct".equals(nodeName)) {
                    } else if
                    ("bucket_averageBitrateKbps".equals(nodeName)) {
                    } else if
                    ("bucket_seekJoinTimeMs".equals(nodeName)) {
                    } else if
                    ("bucket_averageFrameRate".equals(nodeName)) {
                    } else if
                    ("bucket_contentWatchedPct".equals(nodeName)) {
                    } else if (nodeName.startsWith("tags.")) {
                        String key = quote + nodeName.substring(5) + quote;
                        tags.put(key, quote + valueStr + quote);
                    } else {
                        logger.warn(">>> unknown field:" + jn.toString());
                    }


                }

                cs.setTags(tags);


                writer.write(cs.toTSV());

            }
            writer.flush();
            writer.close();


        } catch (IOException ex) {
            System.err.println(ex);
        }
    }


}
