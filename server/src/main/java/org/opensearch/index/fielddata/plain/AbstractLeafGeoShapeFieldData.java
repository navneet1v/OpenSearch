/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.util.Accountable;
import org.opensearch.index.fielddata.*;

import java.util.Collection;
import java.util.Collections;

public abstract class AbstractLeafGeoShapeFieldData implements LeafGeoShapeFieldData {

    /**
     * Return a String representation of the values.
     */
    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return null;
        // return FieldData.toString(getGeoShapeValues());
    }

    /**
     * Returns field values for use in scripting.
     */
    @Override
    public final ScriptDocValues<?> getScriptValues() {
        // return new ScriptDocValues.GeoPoints(getGeoShapeValues());
        return null;
    }

    public static LeafGeoShapeFieldData empty(final int maxDoc) {
        return new AbstractLeafGeoShapeFieldData() {

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public void close() {}

            @Override
            public MultiGeoShapeValues getGeoShapeValues() {
                // send single GeoShape Value from here.
                return null;
            }
        };
    }
}
