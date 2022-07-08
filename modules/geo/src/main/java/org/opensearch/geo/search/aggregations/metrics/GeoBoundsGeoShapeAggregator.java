/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.MultiGeoShapeValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public final class GeoBoundsGeoShapeAggregator extends AbstractGeoBoundsAggregator<ValuesSource.GeoShape> {
    public GeoBoundsGeoShapeAggregator(
        String name,
        SearchContext searchContext,
        Aggregator aggregator,
        ValuesSourceConfig valuesSourceConfig,
        boolean wrapLongitude,
        Map<String, Object> metaData
    ) throws IOException {
        super(name, searchContext, aggregator, valuesSourceConfig, wrapLongitude, metaData);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector leafBucketCollector) {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final MultiGeoShapeValues values = valuesSource.getGeoShapeValues(ctx);
        return new LeafBucketCollectorBase(leafBucketCollector, values) {
            @Override
            public void collect(int doc, long bucket) {
                if (bucket >= tops.size()) {
                    long from = tops.size();
                    tops = bigArrays.grow(tops, bucket + 1);
                    tops.fill(from, tops.size(), Double.NEGATIVE_INFINITY);
                    bottoms = bigArrays.resize(bottoms, tops.size());
                    bottoms.fill(from, bottoms.size(), Double.POSITIVE_INFINITY);
                    posLefts = bigArrays.resize(posLefts, tops.size());
                    posLefts.fill(from, posLefts.size(), Double.POSITIVE_INFINITY);
                    posRights = bigArrays.resize(posRights, tops.size());
                    posRights.fill(from, posRights.size(), Double.NEGATIVE_INFINITY);
                    negLefts = bigArrays.resize(negLefts, tops.size());
                    negLefts.fill(from, negLefts.size(), Double.POSITIVE_INFINITY);
                    negRights = bigArrays.resize(negRights, tops.size());
                    negRights.fill(from, negRights.size(), Double.NEGATIVE_INFINITY);
                }
                tops.set(bucket, -3);
                bottoms.set(bucket, 3);
                posLefts.set(bucket, 3);
                posRights.set(bucket, -3);
                negLefts.set(bucket, 3);
                negRights.set(bucket, -3);
            }
        };
    }
}
