package com.conviva.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.*;

public class KafkaRecordConverterRT {

    public static final char DEFAULT_SEPARATOR = '\t';
    public static final String DEFAULT_LINE_END = "\n";

    public static final String tag_separator_old="\\.";
    public static final String tag_separator_new="_";

    public static final String quote = "'";

    private static long recordcount = 0l;

    static String buildStringArray(String str) {
        return str.replaceAll(quote, "").replaceAll("\"", "\'");
    }

    public static void main(String[] args) {


        String topic = "tlb-rt-sess-summary";
        String groupid = "dytest-rt";

        int sleepintervel = 5;
        String maxpollinter = "30000";

        if (args.length > 0) {
            topic = args[0];
            groupid = args[1];
        }

        Properties props = new Properties();
//        props.put("bootstrap.servers", "rccp109-5d.iad4.prod.conviva.com:30201,rccp110-5c.iad4.prod.conviva.com:30202,rccp111-5b.iad4.prod.conviva.com:30203,rccp111-5d.iad4.prod.conviva.com:30204");

        props.put("bootstrap.servers", "10.72.3.103:9094");
        props.put("group.id", groupid);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");




        Properties props1 = new Properties();
        props1.put("bootstrap.servers",
                "10.72.1.56:9094");
        // props.put("bootstrap.servers", "kafka001.bx.momo.com:9092");
        // props.put("bootstrap.servers", "kafka061.bx:9092");
        props1.put("acks", "1");
        props1.put("retries", 0);
        props1.put("batch.size", 10000);
        props1.put("linger.ms", 2);
        // props.put("buffer.memory", 33554432);
        //
        // props.put("buffer.memory", 33554432);
        // props.put("metadata.fetch.timeout.ms", 5000);

        // props.put("max.request.size", 100); //8m
        // props.put("max.block.ms", 1);

        props1.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props1.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        String topic1 = "ch_experience_insights";

        Producer<String, String> producer = new KafkaProducer<>(props1);


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        consumer.subscribe(Arrays.asList(topic));

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(10000);

            try {
                Thread.sleep(sleepintervel);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }



            for (ConsumerRecord<String, String> record : records) {

                String tsvrecord = parseRecord(record);
                producer.send(new ProducerRecord<String, String>(topic1,
                        null, tsvrecord));

//                System.out.println(tsvrecord);


            }

            System.out.println("get records size: " + records.count());

        }
    }



    public static String parseRecord(ConsumerRecord<String, String> record) {
        try {


            JsonNode jsonTree = new ObjectMapper().readTree(record.value());

            ContentSessionRT cs = new ContentSessionRT();


            Map<String, String> tags = new HashMap<>();

            for (Iterator<Map.Entry<String, JsonNode>> it = jsonTree.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> jn = it.next();

                String nodeName = jn.getKey();
//                    String valueStr = jn.getValue().asText();
                String valueStr = jn.getValue().asText().replaceAll(quote, "").replaceAll(DEFAULT_LINE_END, "").replaceAll("\t", "");


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
                    cs.setAssetName(valueStr.replaceAll(DEFAULT_LINE_END, "").replaceAll("\t", ""));
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
                ("streamingMetadata.realtimeQueryTimestampMs".equals(nodeName)) {
                    cs.setStreamingMetadata_realtimeQueryTimestampMs(Long.parseLong(valueStr));
                } else if
                ("streamingMetadata.watermarkMs".equals(nodeName)) {
                    cs.setStreamingMetadata_watermarkMs(Long.parseLong(valueStr));
                } else if
                ("streamingMetadata.partitionId".equals(nodeName)) {
                    cs.setStreamingMetadata_partitionId(Integer.parseInt(valueStr));
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
                ("switch.adPlayingTimeMs".equals(nodeName)) {
                    cs.setSwitch_adPlayingTimeMs(Integer.parseInt(valueStr));
                } else if
                ("switch.compensationTimeMs".equals(nodeName)) {
                    cs.setSwitch_compensationTimeMs(Integer.parseInt(valueStr));
                } else if
                ("switch.hasPlayingTime".equals(nodeName)) {
                    int i = "true".equals(valueStr) ? 1 : 0;
                    cs.setSwitch_hasPlayingTime(i);
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
                } else if ("tags.m3.dv.hwt".equals(nodeName)) {
                    cs.setM3_dv_hwt(valueStr);
                } else if ("tags.m3.dv.mrk".equals(nodeName)) {
                    cs.setM3_dv_mrk(valueStr);
                }
                else if ("tags.c3.ad.position" == nodeName) cs.setC3_ad_position(valueStr);
                else if ("tags.c3.ad.system" == nodeName) cs.setC3_ad_system(valueStr);
                else if ("tags.c3.ad.technology" == nodeName) cs.setC3_ad_technology(valueStr);
                else if ("tags.c3.ad.isSlate" == nodeName) cs.setC3_ad_isSlate(valueStr);
                else if ("tags.c3.ad.adStitcher" == nodeName) cs.setC3_ad_adStitcher(valueStr);
                else if ("tags.c3.ad.creativeId" == nodeName) cs.setC3_ad_creativeId(valueStr);
                else if ("tags.c3.ad.breakId" == nodeName) cs.setC3_ad_breakId(valueStr);
                else if ("tags.c3.ad.contentAssetName" == nodeName) cs.setC3_ad_contentAssetName(valueStr);
                else if ("tags.c3.pt_ver" == nodeName) cs.setC3_pt_ver(valueStr);
                else if ("tags.c3.device_ua" == nodeName) cs.setC3_device_ua(valueStr);
                else if ("tags.c3.adaptor_type" == nodeName) cs.setC3_adaptor_type(valueStr);
                else if ("tags.c3.protocol.level" == nodeName) cs.setC3_protocol_level(valueStr);
                else if ("tags.c3.protocol.pure" == nodeName) cs.setC3_protocol_pure(valueStr);
                else if ("tags.c3.pt.os.ver" == nodeName) cs.setC3_pt_os_ver(valueStr);
                else if ("tags.c3.device.manufacturer" == nodeName) cs.setC3_device_manufacturer(valueStr);
                else if ("tags.c3.device.brand" == nodeName) cs.setC3_device_brand(valueStr);
                else if ("tags.c3.device.model" == nodeName) cs.setC3_device_model(valueStr);
                else if ("tags.c3.device.conn" == nodeName) cs.setC3_device_conn(valueStr);
                else if ("tags.c3.device.ver" == nodeName) cs.setC3_device_ver(valueStr);
                else if ("tags.c3.go.algoid" == nodeName) cs.setC3_go_algoid(valueStr);
                else if ("tags.c3.player.name" == nodeName) cs.setC3_player_name(valueStr);
                else if ("tags.c3.de.rs.raw" == nodeName) cs.setC3_de_rs_raw(valueStr);
                else if ("tags.c3.de.bitr" == nodeName) cs.setC3_de_bitr(valueStr);
                else if ("tags.c3.de.rsid" == nodeName) cs.setC3_de_rsid(valueStr);
                else if ("tags.c3.de.cdn" == nodeName) cs.setC3_de_cdn(valueStr);
                else if ("tags.c3.de.rs" == nodeName) cs.setC3_de_rs(valueStr);
                else if ("tags.c3.device.cver" == nodeName) cs.setC3_device_cver(valueStr);
                else if ("tags.c3.device.cver.bld" == nodeName) cs.setC3_device_cver_bld(valueStr);
                else if ("tags.c3.video.isLive" == nodeName) {
                    int i = "T" == valueStr ? 1 : 0;
                    cs.setC3_video_isLive(i);
                }
                else if ("tags.c3.viewer.id" == nodeName) cs.setC3_viewer_id(valueStr);
                else if ("tags.c3.client.hwType" == nodeName) cs.setC3_client_hwType(valueStr);
                else if ("tags.c3.client.osname" == nodeName) cs.setC3_client_osname(valueStr);
                else if ("tags.c3.client.manufacturer" == nodeName) cs.setC3_client_manufacturer(valueStr);
                else if ("tags.c3.client.brand" == nodeName) cs.setC3_client_brand(valueStr);
                else if ("tags.c3.client.marketingName" == nodeName) cs.setC3_client_marketingName(valueStr);
                else if ("tags.c3.client.model" == nodeName) cs.setC3_client_model(valueStr);
                else if ("tags.c3.client.osv" == nodeName) cs.setC3_client_osv(valueStr);
                else if ("tags.c3.client.osf" == nodeName) cs.setC3_client_osf(valueStr);
                else if ("tags.c3.client.br" == nodeName) cs.setC3_client_br(valueStr);
                else if ("tags.c3.client.brv" == nodeName) cs.setC3_client_brv(valueStr);
                else if ("tags.m3.dv.mnf" == nodeName) cs.setM3_dv_mnf(valueStr);
                else if ("tags.m3.dv.n" == nodeName) cs.setM3_dv_n(valueStr);
                else if ("tags.m3.dv.os" == nodeName) cs.setM3_dv_os(valueStr);
                else if ("tags.m3.dv.osv" == nodeName) cs.setM3_dv_osv(valueStr);
                else if ("tags.m3.dv.osf" == nodeName) cs.setM3_dv_osf(valueStr);
                else if ("tags.m3.dv.br" == nodeName) cs.setM3_dv_br(valueStr);
                else if ("tags.m3.dv.brv" == nodeName) cs.setM3_dv_brv(valueStr);
                else if ("tags.m3.dv.fw" == nodeName) cs.setM3_dv_fw(valueStr);
                else if ("tags.m3.dv.fwv" == nodeName) cs.setM3_dv_fwv(valueStr);
                else if ("tags.m3.dv.mod" == nodeName) cs.setM3_dv_mod(valueStr);
                else if ("tags.m3.dv.vnd" == nodeName) cs.setM3_dv_vnd(valueStr);
                else if ("tags.m3.net.t" == nodeName) cs.setM3_net_t(valueStr);
                else if ("tags.c3.protocol.type" == nodeName) cs.setC3_protocol_type(valueStr);
                else if ("tags.c3.device.type" == nodeName) cs.setC3_device_type(valueStr);
                else if ("tags.c3.pt.os" == nodeName) cs.setC3_pt_os(valueStr);
                else if ("tags.c3.ft.os" == nodeName) cs.setC3_ft_os(valueStr);
                else if ("tags.c3.framework" == nodeName) cs.setC3_framework(valueStr);
                else if ("tags.c3.framework.ver" == nodeName) cs.setC3_framework_ver(valueStr);
                else if ("tags.c3.pt.br" == nodeName) cs.setC3_pt_br(valueStr);
                else if ("tags.c3.pt.br.ver" == nodeName) cs.setC3_pt_br_ver(valueStr);
                else if ("tags.c3_br.v" == nodeName) cs.setC3_br_v(valueStr);
                else if ("tags.appVersion" == nodeName) cs.setAppVersion(valueStr);
                else if ("tags.network" == nodeName) cs.setNetwork(valueStr);
                else if ("tags.stormflowId" == nodeName) cs.setStormflowId(valueStr);
                else if ("tags.contentType" == nodeName) cs.setContentType(valueStr);
                else if ("tags.m3.dv.cat" == nodeName) cs.setM3_dv_cat(valueStr);
                else if ("tags.c3.cp.an" == nodeName) cs.setC3_cp_an(valueStr);
                else if ("tags.huluPlayerFrameworkName" == nodeName) cs.setHuluPlayerFrameworkName(valueStr);
                else if ("tags.liveSignalProvider" == nodeName) cs.setLiveSignalProvider(valueStr);
                else if ("tags.clientFeatureTags" == nodeName) cs.setClientFeatureTags(valueStr);
                else if ("tags.channel" == nodeName) cs.setChannel(valueStr);
                else if ("tags.huluPlayerFrameworkVersion" == nodeName) cs.setHuluPlayerFrameworkVersion(valueStr);
                else if ("tags.startType" == nodeName) cs.setStartType(valueStr);
                else if ("tags.plt" == nodeName) cs.setPlt(valueStr);
                else if ("tags.DevOps" == nodeName) cs.setDevOps(valueStr);
                else if ("tags.conid" == nodeName) cs.setConid(valueStr);
                else if ("tags.stormflow" == nodeName) cs.setStormflow(valueStr);
                else if (nodeName.startsWith("tags.")) {
//                        String key = quote + nodeName.substring(5) + quote;
//                        tags.put(key, (quote + valueStr.replaceAll(quote, "") + quote).replaceAll(DEFAULT_LINE_END, ""));

                    String key = quote + nodeName.substring(5).replaceAll(tag_separator_old,tag_separator_new) + quote;
                    tags.put(key, (quote + valueStr.replaceAll(quote, "") + quote).replaceAll(DEFAULT_LINE_END, ""));
                } else {
//                        logger.warn(">>> unknown field:" + jn.toString());
                }


            }

            cs.setTags(tags);
            recordcount++;
//               break;
            if (recordcount % 100000 == 0) {
                System.out.println(DateTime.now() + " parsed records :" + recordcount);
            }


            return cs.toSparkTSV();

        } catch (IOException ex) {
            System.err.println(ex);
        }
        return null;
    }
}
