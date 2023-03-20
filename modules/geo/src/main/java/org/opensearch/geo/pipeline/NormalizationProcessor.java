/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.action.search.SearchPhase;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseProcessor;

import java.util.Arrays;
import java.util.Map;

public class NormalizationProcessor implements SearchPhaseProcessor {
    private static final Logger LOG = LogManager.getLogger(NormalizationProcessor.class);

    public static final String NAME = "normalization_processor";

    public NormalizationProcessor() {

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
    public SearchPhaseResults<SearchPhaseResult> execute(
        final SearchPhaseResults<SearchPhaseResult> searchPhaseResults,
        final SearchPhaseContext searchPhaseContext
    ) {
        // this has the all the query results.
        final AtomicArray<SearchPhaseResult> resultAtomicArray = searchPhaseResults.getAtomicArray();
        for (int i = 0; i < resultAtomicArray.length(); i++) {
            final SearchPhaseResult result = resultAtomicArray.get(i);

            if (result.queryResult() != null) {
                final TopDocsAndMaxScore topDocsAndMaxScore = result.queryResult().topDocs();
                final TopDocs topDocs = topDocsAndMaxScore.topDocs;
                final ScoreDoc[] scoreDocs = topDocs.scoreDocs;
                float maxScore = topDocsAndMaxScore.maxScore;
                LOG.info("Current Max Score : " + topDocsAndMaxScore.maxScore);
                for (final ScoreDoc scoreDoc : scoreDocs) {
                    LOG.info("Score Docs is : {} \n", scoreDoc.toString());
                    if (scoreDoc.doc == 1 && scoreDocs.length > 1) {
                        scoreDoc.score = 4;
                    }
                    maxScore = Math.max(maxScore, scoreDoc.score);
                }
                // Sort the topDocs. The way TopDocs merge works in fetch phase exactly similar to merges in Merge
                // Sort. So if a TopDocs list is not sorted, then it can lead to unsorted hits lists.
                Arrays.sort(scoreDocs, (i1, i2) -> (int) (i2.score - i1.score));
                // updating max score
                topDocsAndMaxScore.maxScore = maxScore;
                LOG.info("final Max score for shard is : {}", topDocsAndMaxScore.maxScore);
                LOG.info("Total Hits: {}", topDocs.totalHits);
            } else {
                LOG.info("The QueryResult is null for index {}", i);
            }
        }
        return searchPhaseResults;
    }

    @Override
    public SearchPhase getBeforePhase() {
        return null;
    }

    @Override
    public SearchPhase getAfterPhase() {
        return null;
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
