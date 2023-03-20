/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchPhase;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.search.SearchPhaseResult;

/**
 * Creates a processor that runs between Phases of the Search Request.
 */
public interface SearchPhaseProcessor extends Processor {
    <Result extends SearchPhaseResult> SearchPhaseResults<Result> execute(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext
    );

    SearchPhase getBeforePhase();

    SearchPhase getAfterPhase();

}
