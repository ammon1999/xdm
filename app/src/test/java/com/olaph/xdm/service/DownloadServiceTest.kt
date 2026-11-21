package com.olaph.xdm.service

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class DownloadServiceTest {

    @Test fun summaryNotificationThrottlesSpeedUpdatesToOncePerSecondAndPostsStateChangesImmediately() {
        val throttle = NotificationThrottle(summaryIntervalMs = 1_000L, progressIntervalMs = 800L)
        val base = SummaryKey(running = 1, queued = 0, holdWifi = false, offline = false, limit = 0L)

        assertTrue(throttle.shouldPostSummary(base, speed = 0L, now = 1_000L))
        // Speed ticks faster than 1000ms are suppressed when state is unchanged.
        assertFalse(throttle.shouldPostSummary(base, speed = 1024L, now = 1_250L))
        assertFalse(throttle.shouldPostSummary(base, speed = 2048L, now = 1_999L))
        // At 1000ms with changed speed, posts once.
        assertTrue(throttle.shouldPostSummary(base, speed = 4096L, now = 2_000L))
        // Same speed after 1000ms does not re-post redundantly.
        assertFalse(throttle.shouldPostSummary(base, speed = 4096L, now = 3_500L))
        // State change (e.g. queued count or hold changes) posts immediately regardless of interval.
        val nextState = base.copy(queued = 1)
        assertTrue(throttle.shouldPostSummary(nextState, speed = 4096L, now = 3_510L))
    }

    @Test fun activeAndPausedNotificationsThrottleActiveTicksAndDeduplicatePausedCards() {
        val throttle = NotificationThrottle(summaryIntervalMs = 1_000L, progressIntervalMs = 800L)

        // Paused item that was never shown as active is not posted.
        assertFalse(throttle.shouldPostPaused(10L))

        assertTrue(throttle.shouldPostActive(10L, now = 1_000L))
        assertFalse(throttle.shouldPostActive(10L, now = 1_799L))
        assertTrue(throttle.shouldPostActive(10L, now = 1_800L))

        // Pausing emits one paused card; subsequent ticks do not re-emit it.
        assertTrue(throttle.shouldPostPaused(10L))
        assertFalse(throttle.shouldPostPaused(10L))
        assertFalse(throttle.shouldPostPaused(10L))

        // Resuming re-arms the paused card and posts immediately because lastPosted was cleared on pause.
        assertTrue(throttle.shouldPostActive(10L, now = 1_900L))
        assertTrue(throttle.shouldPostPaused(10L))
        assertFalse(throttle.shouldPostPaused(10L))
    }

    @Test fun pruneClosedCancelsAndResetsTrackingForRemovedIds() {
        val throttle = NotificationThrottle()
        assertTrue(throttle.shouldPostActive(1L, now = 100L))
        assertTrue(throttle.shouldPostActive(2L, now = 100L))
        assertTrue(throttle.shouldPostPaused(2L))

        assertEquals(listOf(2L), throttle.pruneClosed(setOf(1L)))
        // After pruning id 2, paused cannot fire until shown as active again.
        assertFalse(throttle.shouldPostPaused(2L))
        assertTrue(throttle.shouldPostPaused(1L))
    }
}
