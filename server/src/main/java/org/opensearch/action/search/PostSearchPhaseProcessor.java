/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.action.search;

import org.opensearch.search.SearchPhaseResult;

/**
 * Defines contract for PostSearch phase processor
 */
public interface PostSearchPhaseProcessor {

    <Result extends SearchPhaseResult> SearchPhaseResults<Result> execute(SearchPhaseResults<Result> results, SearchPhaseContext context);
}
