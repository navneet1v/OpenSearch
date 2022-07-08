/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geometry.docvalues;

import org.apache.lucene.document.Reader;
import org.apache.lucene.document.ShapeDocValuesField;
import org.apache.lucene.util.BytesRef;

/**
 * This class represents a converted doc value from Lucene index to Geo Shape which we can use in the Aggregation.
 */
public class GeometryShape {
    private final ShapeDocValuesField shapeDocValuesField;
    private final Reader reader;

    public GeometryShape(final String fieldName, BytesRef bytesRef) {
        this.reader = new Reader(bytesRef);
        this.shapeDocValuesField = new ShapeDocValuesField(fieldName, bytesRef);
    }
}
