package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.cache.CacheStore;

public class EnrichmentCacheManager extends NullValuesCacheManager {

    public EnrichmentCacheManager() {
        CacheStore primaryCache = new CacheStore();
        // set expiration to one day.
        primaryCache.setEntryExpiryDurationInMillis(24 * 60 * 60 * 1000);
        primaryCache.setCacheCleanupInMillis(24 * 60 * 60 * 1000);
        primaryCache.setEntryExpiryStrategy(CacheStore.ExpiryType.EXPIRE_AFTER_WRITE);
        primaryCache.setMaxCacheSize(16 * 1024 * 1024);

        super.setPrimary(primaryCache);
    }
}
