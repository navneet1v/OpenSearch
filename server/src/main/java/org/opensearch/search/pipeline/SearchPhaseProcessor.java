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

    /**
     * Validates before calling the execute method of the SearchPhaseProcessor to see whether we need to run the
     * processor or not.
     * @param searchPhaseResult {@link SearchPhaseResults}
     * @param searchPhaseContext {@link SearchPhaseContext}
     * @param beforePhase {@link SearchPhase}
     * @param nextPhase {@link SearchPhase}
     * @return boolean
     * @param <Result> {@link SearchPhaseResult}
     */
    <Result extends SearchPhaseResult> boolean runProcessor(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext,
        final SearchPhase beforePhase,
        final SearchPhase nextPhase
    );

}
