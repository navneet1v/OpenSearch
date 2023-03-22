/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.CompoundQueryTopDocs;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.query.QuerySearchResult;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of PostSearchPhaseProcessor that does score normalization and combination
 */
public class NormalizationPostSearchPhaseProcessor implements PostSearchPhaseProcessor {
    private static final Logger log = LogManager.getLogger(NormalizationPostSearchPhaseProcessor.class);

    static float MIN_SCORE = 0.001f;

    static float SINGLE_RESULT_SCORE = 1.0f;

    static TriFunction<Float, Float, Float, Float> normalizeScoreFunction = (score, minScore, maxScore) -> {
        // edge case when there is only one score and min and max scores are same
        if (maxScore == minScore && maxScore == score) {
            return SINGLE_RESULT_SCORE;
        }
        float normalizedScore = (score - minScore) / (maxScore - minScore);
        return normalizedScore == 0.0f ? MIN_SCORE : normalizedScore;
    };

    static Function<float[], Float> combineScoreFunction = scores -> {
        float combinedScore = 0.0f;
        int count = 0;
        for (float score : scores) {
            if (score >= 0.0) {
                combinedScore += score;
                count++;
            }
        }
        return combinedScore / count;
    };

    @Override
    public <Result extends SearchPhaseResult> SearchPhaseResults<Result> execute(
        SearchPhaseResults<Result> results,
        SearchPhaseContext context
    ) {
        log.warn("PostSearchPhaseProcessor.execute");
        TopDocsAndMaxScore[] topDocsAndMaxScores = getCompoundQueryTopDocsForResult(results);
        CompoundQueryTopDocs[] queryTopDocs = Arrays.stream(topDocsAndMaxScores)
            .map(td -> td != null ? (CompoundQueryTopDocs) td.topDocs : null)
            .collect(Collectors.toList())
            .toArray(CompoundQueryTopDocs[]::new);

        // normalization
        doNormalization(queryTopDocs);

        // combination
        for (int i = 0; i < queryTopDocs.length; i++) {
            CompoundQueryTopDocs compoundQueryTopDocs = queryTopDocs[i];
            if (compoundQueryTopDocs == null) {
                continue;
            }
            TopDocs[] topDocsPerSubQuery = compoundQueryTopDocs.getDocs();
            int shardId = compoundQueryTopDocs.scoreDocs[0].shardIndex;
            Map<Integer, float[]> normalizedScoresPerDoc = new HashMap<>();
            int maxHits = 0;
            for (int j = 0; j < topDocsPerSubQuery.length; j++) {
                TopDocs topDocs = topDocsPerSubQuery[j];
                int hits = 0;
                log.warn(String.format("totalHits: %s", topDocs.totalHits.toString()));
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    if (!normalizedScoresPerDoc.containsKey(scoreDoc.doc)) {
                        float[] scores = new float[topDocsPerSubQuery.length];
                        // we initialize with -1.0, as after normalization it's possible that score is 0.0
                        Arrays.fill(scores, -1.0f);
                        normalizedScoresPerDoc.put(scoreDoc.doc, scores);
                    }
                    normalizedScoresPerDoc.get(scoreDoc.doc)[j] = scoreDoc.score;
                    hits++;
                }
                maxHits = Math.max(maxHits, hits);
            }
            Map<Integer, Float> combinedNormalizedScoresByDocId = normalizedScoresPerDoc.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> combineScoreFunction.apply(entry.getValue())));
            // create priority queue, make it max heap by the score
            PriorityQueue<Integer> pq = new PriorityQueue<>(
                (a, b) -> Float.compare(combinedNormalizedScoresByDocId.get(b), combinedNormalizedScoresByDocId.get(a))
            );
            // we're merging docs with normalized and combined scores. we need to have only maxHits results
            for (int docId : normalizedScoresPerDoc.keySet()) {
                pq.add(docId);
            }

            ScoreDoc[] finalScoreDocs = new ScoreDoc[maxHits];
            float maxScore = combinedNormalizedScoresByDocId.get(pq.peek());

            for (int j = 0; j < maxHits; j++) {
                int docId = pq.poll();
                finalScoreDocs[j] = new ScoreDoc(docId, combinedNormalizedScoresByDocId.get(docId), shardId);
            }
            compoundQueryTopDocs.scoreDocs = finalScoreDocs;
            compoundQueryTopDocs.totalHits = new TotalHits(maxHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            log.warn(
                String.format("update top docs maxScore, original value: %f, updated value %f", topDocsAndMaxScores[i].maxScore, maxScore)
            );
            topDocsAndMaxScores[i].maxScore = maxScore;
        }

        updateOriginalQueryResults(results, queryTopDocs);

        return results;
    }

    private void doNormalization(CompoundQueryTopDocs[] queryTopDocs) {
        Optional<CompoundQueryTopDocs> maybeCompoundQuery = Arrays.stream(queryTopDocs).filter(Objects::nonNull).findAny();
        if (maybeCompoundQuery.isEmpty()) {
            return;
        }

        // init scores per sub-query
        float[][] minMaxScores = new float[maybeCompoundQuery.get().getDocs().length][];
        for (int i = 0; i < minMaxScores.length; i++) {
            minMaxScores[i] = new float[] { Float.MAX_VALUE, Float.MIN_VALUE };
        }

        for (CompoundQueryTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (compoundQueryTopDocs == null) {
                continue;
            }
            TopDocs[] topDocsPerSubQuery = compoundQueryTopDocs.getDocs();
            /*log.warn(
                String.format(
                    "Result for one sub-query from compound query: %d",
                    topDocsPerSubQuery == null ? 0 : topDocsPerSubQuery.length
                )
            );*/
            for (int j = 0; j < topDocsPerSubQuery.length; j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery[j];
                log.warn(String.format("totalHits: %s", subQueryTopDoc.totalHits.toString()));
                // get min and max scores
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    minMaxScores[j][0] = Math.min(minMaxScores[j][0], scoreDoc.score);
                    minMaxScores[j][1] = Math.max(minMaxScores[j][1], scoreDoc.score);
                    log.warn(String.format("Score doc : %s", scoreDoc));
                }
            }
        }
        // do the normalization
        for (CompoundQueryTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (compoundQueryTopDocs == null) {
                continue;
            }
            TopDocs[] topDocsPerSubQuery = compoundQueryTopDocs.getDocs();
            for (int j = 0; j < topDocsPerSubQuery.length; j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery[j];
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    scoreDoc.score = normalizeScoreFunction.apply(scoreDoc.score, minMaxScores[j][0], minMaxScores[j][1]);
                }
            }
        }
    }

    private <Result extends SearchPhaseResult> void updateOriginalQueryResults(
        SearchPhaseResults<Result> results,
        CompoundQueryTopDocs[] queryTopDocs
    ) {
        List<Result> preShardResultList = results.getAtomicArray().asList();
        for (int i = 0; i < preShardResultList.size(); i++) {
            QuerySearchResult querySearchResult = preShardResultList.get(i).queryResult();
            CompoundQueryTopDocs updatedTopDocs = queryTopDocs[i];
            if (updatedTopDocs == null) {
                continue;
            }
            float maxScore = updatedTopDocs.totalHits.value > 0 ? updatedTopDocs.scoreDocs[0].score : 0.0f;
            TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(updatedTopDocs, maxScore);
            querySearchResult.topDocs(topDocsAndMaxScore, null);
        }
    }

    <Result extends SearchPhaseResult> TopDocsAndMaxScore[] getCompoundQueryTopDocsForResult(SearchPhaseResults<Result> results) {
        List<Result> preShardResultList = results.getAtomicArray().asList();
        TopDocsAndMaxScore[] result = new TopDocsAndMaxScore[results.getAtomicArray().length()];
        int idx = 0;
        for (Result shardResult : preShardResultList) {
            if (shardResult == null) {
                idx++;
                continue;
            }
            QuerySearchResult querySearchResult = shardResult.queryResult();
            TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
            if (!(topDocsAndMaxScore.topDocs instanceof CompoundQueryTopDocs)) {
                idx++;
                continue;
            }
            result[idx++] = topDocsAndMaxScore;
        }
        return result;
    }

    @FunctionalInterface
    interface TriFunction<A, B, C, R> {

        R apply(A a, B b, C c);

        default <V> TriFunction<A, B, C, V> andThen(Function<? super R, ? extends V> after) {
            Objects.requireNonNull(after);
            return (A a, B b, C c) -> after.apply(apply(a, b, c));
        }
    }
}
