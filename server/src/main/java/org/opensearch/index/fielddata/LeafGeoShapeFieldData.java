/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

/**
 * {@link LeafFieldData} specialization for geo shapes.
 *
 * @opensearch.internal
 */
public interface LeafGeoShapeFieldData extends LeafFieldData {

    /**
     * Return GeoShapeValues.
     * @return MultiGeoShapeValues
     */
    MultiGeoShapeValues getGeoShapeValues();
}
