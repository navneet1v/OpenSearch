/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

/**
 * OpenSearch plugin that provides IO-uring based index storage.
 *
 * <p>Usage: Set {@code index.store.type: io_uring} in index settings.</p>
 */
public class IOUringStorePlugin extends Plugin implements IndexStorePlugin {

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + System.getProperty("java.library.path"));
        return Collections.singletonMap("io_uring", new IOUringDirectoryFactory());
    }

    /**
     * Factory for creating IOUringDirectory instances.
     */
    public static class IOUringDirectoryFactory implements DirectoryFactory {

        @Override
        public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
            return new IOUringDirectory(shardPath.resolveIndex());
        }

        @Override
        public Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
            return new IOUringDirectory(location, lockFactory);
        }
    }
}
