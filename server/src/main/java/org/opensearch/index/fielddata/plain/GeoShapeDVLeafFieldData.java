/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.opensearch.geometry.docvalues.GeometryShape;
import org.opensearch.index.fielddata.MultiGeoShapeValues;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * GeoShape/Shape field data
 * This is the class that converts the DocValue field of the GeoShape to the right internal representation
 * @opensearch.internal
 */
public class GeoShapeDVLeafFieldData extends AbstractLeafGeoShapeFieldData {

    private final LeafReader reader;
    private final String fieldName;

    GeoShapeDVLeafFieldData(final LeafReader reader, String fieldName) {
        super();
        this.reader = reader;
        this.fieldName = fieldName;
    }

    /**
     * Return the memory usage of this object in bytes. Negative values are illegal.
     */
    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public void close() {
        // noop
    }

    /**
     * Returns nested resources of this class. The result should be a point-in-time snapshot (to avoid
     * race conditions).
     *
     * @see Accountables
     */
    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    /**
     * Return GeoShapeValues.
     *
     * @return MultiGeoShapeValues
     */
    @Override
    public MultiGeoShapeValues getGeoShapeValues() {
        try {
            final BinaryDocValues binaryDocValues = DocValues.getBinary(reader, fieldName);

            return new MultiGeoShapeValues() {
                @Override
                public boolean advanceExact(int doc) throws IOException {
                    // binaryDocValues.nextDoc()
                    return binaryDocValues.advanceExact(doc);
                }

                @Override
                public GeometryShape nextValue() throws IOException {
                    final BytesRef bytesRef = binaryDocValues.binaryValue();
                    // convert this byteref to the correct docValue the way we have stored in the
                    // Field Mapper.
                    return new GeometryShape(fieldName, bytesRef);
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load GeoShapeValues", e);
        }
    }
}
