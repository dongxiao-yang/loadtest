package com.conviva.clickhouse;

public class Test {

    public   static      String[] columnames = {
            "version",
            "customerId",
            "clientId",
            "sessionId",
            "segmentId",
            "datasourceId",
            "isAudienceOnly",
            "isAd",
            "assetName",
            "streamUrl",
            "contentLengthMs",
            "ipType",
            "geo.continent",
            "geo.country",
            "geo.state",
            "geo.city",
            "geo.dma",
            "geo.asn",
            "geo.isp",
            "geo.postalCode",
            "life.firstReceivedTimeMs",
            "life.latestReceivedTimeMs",
            "life.sessionTimeMs",
            "life.playingTimeMs",
            "life.bufferingTimeMs",
            "life.networkBufferingTimeMs",
            "life.rebufferingRatioPct",
            "life.networkRebufferingRatioPct",
            "life.averageBitrateKbps",
            "life.seekJoinTimeMs",
            "life.seekJoinCount",
            "life.bufferingEvents",
            "life.networkRebufferingEvents",
            "life.bitrateKbps",
            "life.contentWatchedTimeMs",
            "life.contentWatchedPct",
            "life.averageFrameRate",
            "life.renderingQuality",
            "life.resourceIds",
            "life.cdns",
            "life.fatalErrorResourceIds",
            "life.fatalErrorCdns",
            "life.joinResourceIds",
            "life.joinCdns",
            "life.lastJoinCdn",
            "life.lastCdn",
            "life.lastJoinResourceId",
            "life.isVideoPlaybackFailure",
            "life.isVideoStartFailure",
            "life.hasJoined",
            "life.isVideoPlaybackFailureBusiness",
            "life.isVideoPlaybackFailureTech",
            "life.isVideoStartFailureBusiness",
            "life.isVideoStartFailureTech",
            "life.videoPlaybackFailureErrorsBusiness",
            "life.videoPlaybackFailureErrorsTech",
            "life.videoStartFailureErrorsBusiness",
            "life.videoStartFailureErrorsTech",
            "life.exitDuringPreRoll",
            "interval.startTimeMs",
            "switch.resourceId",
            "switch.cdn",
            "switch.justJoined",
            "switch.hasJoined",
            "switch.justJoinedAndLifeJoinTimeMsIsAccurate",
            "switch.isEndedPlay",
            "switch.isEnded",
            "switch.isEndedPlayAndLifeAverageBitrateKbpsGT0",
            "switch.isVideoStartFailure",
            "switch.videoStartFailureErrors",
            "switch.isExitBeforeVideoStart",
            "switch.isVideoPlaybackFailure",
            "switch.isVideoStartSave",
            "switch.videoPlaybackFailureErrors",
            "switch.isAttempt",
            "switch.playingTimeMs",
            "switch.rebufferingTimeMs",
            "switch.networkRebufferingTimeMs",
            "switch.rebufferingDuringAdsMs",
            "switch.adRelatedBufferingMs",
            "switch.bitrateBytes",
            "switch.bitrateTimeMs",
            "switch.framesLoaded",
            "switch.framesPlayingTimeMs",
            "switch.seekJoinTimeMs",
            "switch.seekJoinCount",
            "switch.isVideoPlaybackFailureBusiness",
            "switch.isVideoPlaybackFailureTech",
            "switch.isVideoStartFailureBusiness",
            "switch.isVideoStartFailureTech",
            "switch.videoPlaybackFailureErrorsBusiness",
            "switch.videoPlaybackFailureErrorsTech",
            "switch.videoStartFailureErrorsBusiness",
            "switch.videoStartFailureErrorsTech",
            "switch.adRequested",
            "bucket.sessionTimeMs",
            "bucket.playingTimeMs",
            "bucket.bufferingTimeMs",
            "bucket.networkBufferingTimeMs",
            "bucket.rebufferingRatioPct",
            "bucket.networkRebufferingRatioPct",
            "bucket.averageBitrateKbps",
            "bucket.seekJoinTimeMs",
            "bucket.averageFrameRate",
            "bucket.contentWatchedPct",
            "tags.key",
            "tags.value"};
    public static void main(String[] args) {

        for (String str : columnames)
        {

            String cname = "";
            if(str.contains("."))
            {
                String[] aaa =  str.split("\\.");
                cname =  String.join("_",aaa);

            }
            else
            {
                cname = str;
            }
            System.out.println("public String " +cname + ";");
        }
    }
}


