/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.MultiGeoPointValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregate all docs into a geographic bounds
 *
 * @opensearch.internal
 */
public class GeoBoundsAggregator extends AbstractGeoBoundsAggregator<ValuesSource.GeoPoint> {

    public GeoBoundsAggregator(
        String name,
        SearchContext aggregationContext,
        Aggregator parent,
        ValuesSourceConfig valuesSourceConfig,
        boolean wrapLongitude,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, aggregationContext, parent, valuesSourceConfig, wrapLongitude, metadata);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final MultiGeoPointValues values = valuesSource.geoPointValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
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

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0; i < valuesCount; ++i) {
                        GeoPoint value = values.nextValue();
                        double top = tops.get(bucket);
                        if (value.lat() > top) {
                            top = value.lat();
                        }
                        double bottom = bottoms.get(bucket);
                        if (value.lat() < bottom) {
                            bottom = value.lat();
                        }
                        double posLeft = posLefts.get(bucket);
                        if (value.lon() >= 0 && value.lon() < posLeft) {
                            posLeft = value.lon();
                        }
                        double posRight = posRights.get(bucket);
                        if (value.lon() >= 0 && value.lon() > posRight) {
                            posRight = value.lon();
                        }
                        double negLeft = negLefts.get(bucket);
                        if (value.lon() < 0 && value.lon() < negLeft) {
                            negLeft = value.lon();
                        }
                        double negRight = negRights.get(bucket);
                        if (value.lon() < 0 && value.lon() > negRight) {
                            negRight = value.lon();
                        }
                        tops.set(bucket, top);
                        bottoms.set(bucket, bottom);
                        posLefts.set(bucket, posLeft);
                        posRights.set(bucket, posRight);
                        negLefts.set(bucket, negLeft);
                        negRights.set(bucket, negRight);
                    }
                }
            }
        };
    }
}
