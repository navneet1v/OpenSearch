/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.opensearch.geometry.docvalues.GeometryShape;

import java.io.IOException;

/**
 * A stateful lightweight per document set of {@link org.opensearch.geometry.Geometry} values.
 * We are not converting things to geometry, we will send the data as {@link org.apache.lucene.geo.Tessellator.Triangle}, as this
 * is how we have saved the data. Check {@link org.opensearch.index.mapper.GeoShapeFieldMapper}.addDocValuesFields method
 * <p>
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   MultiShapeValues values = ..;
 *   values.setDocId(docId);
 *   final int numValues = values.count();
 *   for (int i = 0; i &lt; numValues; i++) {
 *       Geometry value = values.valueAt(i);
 *       // process value
 *   }
 * </pre>
 * The set of values associated with a document might contain duplicates and
 * comes in a non-specified order.
 *
 * @opensearch.internal
 */
public abstract class MultiGeoShapeValues {
    /**
     * Creates a new {@link MultiGeoShapeValues} instance
     */
    protected MultiGeoShapeValues() {}

    /**
     * Advance this instance to the given document id
     *
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Return the next value associated with the current document.
     *
     * @return the next value for the current docID set to {@link #advanceExact(int)}.
     */
    public abstract GeometryShape nextValue() throws IOException;

    public static class EmptyGeoShapeValue extends MultiGeoShapeValues {
        /**
         * Advance this instance to the given document id
         *
         * @param doc int
         * @return true if there is a value for this document
         */
        @Override
        public boolean advanceExact(int doc) throws IOException {
            return false;
        }

        /**
         * Return the next value associated with the current document.
         *
         * @return the next value for the current docID set to {@link #advanceExact(int)}.
         */
        @Override
        public GeometryShape nextValue() throws IOException {
            throw new UnsupportedOperationException("This empty geoShape value, hence this operation is not supported");
        }
    }
}
