/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.pipeline;

import org.opensearch.action.search.NormalizationPostSearchPhaseProcessor;
import org.opensearch.action.search.PostSearchPhaseProcessor;
import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhase;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.common.lucene.search.CompoundQueryTopDocs;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseProcessor;

import java.util.Map;
import java.util.Optional;

/**
 * This normalization processor should only run for Normalization Query Clauses. We need to find a way to do that. We
 * don't want to run this for every Query Clause.
 */
public class NormalizationProcessor implements SearchPhaseProcessor {
    public static final String NAME = "normalization_processor";
    private final PostSearchPhaseProcessor postSearchPhaseProcessor;

    public NormalizationProcessor() {
        postSearchPhaseProcessor = new NormalizationPostSearchPhaseProcessor();
    }

    /**
     * Gets the type of processor
     */
    @Override
    public String getType() {
        return "normalization_processor";
    }

    /**
     * Gets the tag of a processor.
     */
    @Override
    public String getTag() {
        return "normalization_processor";
    }

    /**
     * Gets the description of a processor.
     */
    @Override
    public String getDescription() {
        return "This processor does Normalization for the Scores";
    }

    @Override
    public <Result extends SearchPhaseResult> SearchPhaseResults<Result> execute(
        final SearchPhaseResults<Result> searchPhaseResults,
        final SearchPhaseContext searchPhaseContext
    ) {
        if (searchPhaseResults instanceof QueryPhaseResultConsumer) {
            QueryPhaseResultConsumer queryPhaseResultConsumer = (QueryPhaseResultConsumer) searchPhaseResults;
            Optional<SearchPhaseResult> maybeResult = Optional.empty();
            for (int i = 0; i < queryPhaseResultConsumer.getAtomicArray().length(); i++) {
                if (queryPhaseResultConsumer.getAtomicArray().get(i) != null) {
                    maybeResult = Optional.of(queryPhaseResultConsumer.getAtomicArray().get(i));
                    // why we are breaking here? we should just bypass.
                    break;
                }
            }
            // this seems like it works if all shards have provided the result. Good for POC but not for production
            if (maybeResult.isPresent()
                && maybeResult.get().queryResult() != null
                && maybeResult.get().queryResult().topDocs().topDocs instanceof CompoundQueryTopDocs) {
                postSearchPhaseProcessor.execute(searchPhaseResults, searchPhaseContext);
            }
        }
        return searchPhaseResults;
    }

    @Override
    public <Result extends SearchPhaseResult> boolean runProcessor(
        SearchPhaseResults<Result> searchPhaseResult,
        SearchPhaseContext searchPhaseContext,
        SearchPhase beforePhase,
        SearchPhase nextPhase
    ) {
        return "query".equals(beforePhase.getName()) && "fetch".equals(nextPhase.getName());
    }

    public static class NormalizationProcessorFactory implements Processor.Factory {

        /**
         * Creates a processor based on the specified map of maps config.
         *
         * @param processorFactories Other processors which may be created inside this processor
         * @param tag                The tag for the processor
         * @param description        A short description of what this processor does
         * @param config             The configuration for the processor
         *
         *                           <b>Note:</b> Implementations are responsible for removing the used configuration keys, so that after
         *                           creation the config map should be empty.
         */
        @Override
        public Processor create(Map<String, Factory> processorFactories, String tag, String description, Map<String, Object> config)
            throws Exception {
            return new NormalizationProcessor();
        }
    }
}
