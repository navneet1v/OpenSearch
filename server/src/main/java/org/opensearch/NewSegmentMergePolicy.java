/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch;

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

import java.io.IOException;
import java.util.Map;

/**
 * Rnad doc
 */
public class NewSegmentMergePolicy extends FilterMergePolicy {
    private boolean mergeCompleted;
    public NewSegmentMergePolicy(MergePolicy in) {
        super(in);
        mergeCompleted = false;
    }


    /**
     * Rnad doc
     */
    @Override
    public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
        if(mergeCompleted) {
            return null;
        }
        mergeCompleted = true;
        MergeSpecification mergeSpecification = new MergeSpecification();
        mergeSpecification.add(new MergePolicy.OneMerge(segmentInfos.asList()));
        return mergeSpecification;
    }
}
