/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.asynchronous.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

import static org.opensearch.common.unit.TimeValue.timeValueDays;
import static org.opensearch.common.unit.TimeValue.timeValueHours;
import static org.opensearch.common.unit.TimeValue.timeValueMinutes;
import static org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES;

public class LegacyOpendistroAsynchronousSearchSettings {

    public static final Setting<Integer> NODE_CONCURRENT_RUNNING_SEARCHES_SETTING = Setting.intSetting(
            "opendistro.asynchronous_search.node_concurrent_running_searches", NODE_CONCURRENT_RUNNING_SEARCHES, 0,
            Setting.Property.Dynamic, Setting.Property.NodeScope, Setting.Property.Deprecated);

    public static final Setting<TimeValue> MAX_KEEP_ALIVE_SETTING =
            Setting.positiveTimeSetting("opendistro.asynchronous_search.max_keep_alive", timeValueDays(5),
                    Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated);
    public static final Setting<TimeValue> MAX_SEARCH_RUNNING_TIME_SETTING =
            Setting.positiveTimeSetting("opendistro.asynchronous_search.max_search_running_time", timeValueHours(12),
                    Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated);
    public static final Setting<TimeValue> MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING = Setting.positiveTimeSetting(
            "opendistro.asynchronous_search.max_wait_for_completion_timeout", timeValueMinutes(1), Setting.Property.NodeScope,
            Setting.Property.Dynamic, Setting.Property.Deprecated);

    public static final Setting<Boolean> PERSIST_SEARCH_FAILURES_SETTING =
            Setting.boolSetting("opendistro.asynchronous_search.persist_search_failures", false,
                    Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated);

    public static final Setting<TimeValue> ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING =
            Setting.timeSetting("opendistro.asynchronous_search.active.context.reaper_interval", TimeValue.timeValueMinutes(5),
                    TimeValue.timeValueSeconds(5),
                    Setting.Property.NodeScope, Setting.Property.Deprecated);
    public static final Setting<TimeValue> PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING =
            Setting.timeSetting("opendistro.asynchronous_search.expired.persisted_response.cleanup_interval",
                    TimeValue.timeValueMinutes(30), TimeValue.timeValueSeconds(5),
                    Setting.Property.NodeScope, Setting.Property.Deprecated);
}
