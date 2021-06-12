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

import org.junit.Before;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import org.opensearch.search.asynchronous.management.AsynchronousSearchManagementService;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.search.asynchronous.service.AsynchronousSearchService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings({"rawtypes"})
public class AsynchronousSearchSettingsTests extends OpenSearchTestCase {
    AsynchronousSearchPlugin plugin;

    @Before
    public void setup() {
        this.plugin = new AsynchronousSearchPlugin();
    }

    public void testAllLegacyOpenDistroSettingsReturned() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue(
                "legacy setting must be returned from settings",
                settings.containsAll(Arrays.asList(
                        LegacyOpendistroAsynchronousSearchSettings.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING,
                        LegacyOpendistroAsynchronousSearchSettings.MAX_KEEP_ALIVE_SETTING,
                        LegacyOpendistroAsynchronousSearchSettings.MAX_SEARCH_RUNNING_TIME_SETTING,
                        LegacyOpendistroAsynchronousSearchSettings.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING,
                        LegacyOpendistroAsynchronousSearchSettings.PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING,
                        LegacyOpendistroAsynchronousSearchSettings.ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING,
                        LegacyOpendistroAsynchronousSearchSettings.PERSIST_SEARCH_FAILURES_SETTING
                )));
    }

    public void testAllOpenSearchSettingsReturned() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue(
                "legacy setting must be returned from settings",
                settings.containsAll(Arrays.asList(
                        AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING,
                        AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING,
                        AsynchronousSearchManagementService.PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING,
                        AsynchronousSearchManagementService.ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING,
                        AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING
                )));
    }

    public void testAllLegacyOpenDistroSettingsFallback() {
        assertEquals(
                AsynchronousSearchManagementService.ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING.get(Settings.EMPTY),
                LegacyOpendistroAsynchronousSearchSettings.ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING.get(Settings.EMPTY)
        );
        assertEquals(
                AsynchronousSearchManagementService.PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING.get(Settings.EMPTY),
                LegacyOpendistroAsynchronousSearchSettings.PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING.get(Settings.EMPTY)
        );
        assertEquals(
                AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.get(Settings.EMPTY),
                LegacyOpendistroAsynchronousSearchSettings.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.get(Settings.EMPTY)
        );
        assertEquals(
                AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING.get(Settings.EMPTY),
                LegacyOpendistroAsynchronousSearchSettings.MAX_KEEP_ALIVE_SETTING.get(Settings.EMPTY)
        );
        assertEquals(
                AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING.get(Settings.EMPTY),
                LegacyOpendistroAsynchronousSearchSettings.MAX_SEARCH_RUNNING_TIME_SETTING.get(Settings.EMPTY)
        );
        assertEquals(
                AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.get(Settings.EMPTY),
                LegacyOpendistroAsynchronousSearchSettings.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.get(Settings.EMPTY)
        );
        assertEquals(
                AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING.get(Settings.EMPTY),
                LegacyOpendistroAsynchronousSearchSettings.PERSIST_SEARCH_FAILURES_SETTING.get(Settings.EMPTY)
        );
    }

    public void testSettingsGetValue() {
        Settings settings =
                Settings.builder().put(AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(), "200").build();
        assertEquals(AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.get(settings).intValue(), 200);
        assertEquals(LegacyOpendistroAsynchronousSearchSettings.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.get(settings).intValue(), 20);

        settings =
                Settings.builder().put(AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING.getKey(), "10d").build();
        assertEquals(AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING.get(settings), TimeValue.timeValueDays(10));
        assertEquals(LegacyOpendistroAsynchronousSearchSettings.MAX_KEEP_ALIVE_SETTING.get(settings), TimeValue.timeValueDays(5));

        settings =
                Settings.builder().put(AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING.getKey(), "2d").build();
        assertEquals(AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING.get(settings), TimeValue.timeValueDays(2));
        assertEquals(LegacyOpendistroAsynchronousSearchSettings.MAX_SEARCH_RUNNING_TIME_SETTING.get(settings),
                TimeValue.timeValueHours(12));

        settings =
                Settings.builder().put(AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.getKey(), "2m").build();
        assertEquals(AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.get(settings), TimeValue.timeValueMinutes(2));
        assertEquals(LegacyOpendistroAsynchronousSearchSettings.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.get(settings),
                TimeValue.timeValueMinutes(1));

        settings =
                Settings.builder().put(AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING.getKey(), "true").build();
        assertEquals(AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING.get(settings), true);
        assertEquals(LegacyOpendistroAsynchronousSearchSettings.PERSIST_SEARCH_FAILURES_SETTING.get(settings), false);
    }
}
