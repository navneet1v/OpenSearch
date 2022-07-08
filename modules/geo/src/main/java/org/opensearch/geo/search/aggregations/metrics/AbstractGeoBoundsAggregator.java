/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.metrics;

import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalGeoBounds;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractGeoBoundsAggregator<T extends ValuesSource> extends MetricsAggregator {

    protected final T valuesSource;
    protected final boolean wrapLongitude;
    protected DoubleArray tops;
    protected DoubleArray bottoms;
    protected DoubleArray posLefts;
    protected DoubleArray posRights;
    protected DoubleArray negLefts;
    protected DoubleArray negRights;

    @SuppressWarnings("unchecked")
    protected AbstractGeoBoundsAggregator(
        String name,
        SearchContext searchContext,
        Aggregator aggregator,
        ValuesSourceConfig valuesSourceConfig,
        boolean wrapLongitude,
        Map<String, Object> metaData
    ) throws IOException {
        super(name, searchContext, aggregator, metaData);
        this.wrapLongitude = wrapLongitude;
        valuesSource = valuesSourceConfig.hasValues() ? (T) valuesSourceConfig.getValuesSource() : null;

        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            tops = bigArrays.newDoubleArray(1, false);
            tops.fill(0, tops.size(), Double.NEGATIVE_INFINITY);
            bottoms = bigArrays.newDoubleArray(1, false);
            bottoms.fill(0, bottoms.size(), Double.POSITIVE_INFINITY);
            posLefts = bigArrays.newDoubleArray(1, false);
            posLefts.fill(0, posLefts.size(), Double.POSITIVE_INFINITY);
            posRights = bigArrays.newDoubleArray(1, false);
            posRights.fill(0, posRights.size(), Double.NEGATIVE_INFINITY);
            negLefts = bigArrays.newDoubleArray(1, false);
            negLefts.fill(0, negLefts.size(), Double.POSITIVE_INFINITY);
            negRights = bigArrays.newDoubleArray(1, false);
            negRights.fill(0, negRights.size(), Double.NEGATIVE_INFINITY);
        }
    }

    /**
     * Build an empty aggregation.
     */
    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoBounds(
            name,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            wrapLongitude,
            metadata()
        );
    }

    /**
     * Build an aggregation for data that has been collected into
     * {@code owningBucketOrd}.
     */
    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        if (valuesSource == null) {
            return buildEmptyAggregation();
        }
        double top = tops.get(owningBucketOrdinal);
        double bottom = bottoms.get(owningBucketOrdinal);
        double posLeft = posLefts.get(owningBucketOrdinal);
        double posRight = posRights.get(owningBucketOrdinal);
        double negLeft = negLefts.get(owningBucketOrdinal);
        double negRight = negRights.get(owningBucketOrdinal);
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(tops, bottoms, posLefts, posRights, negLefts, negRights);
    }
}
