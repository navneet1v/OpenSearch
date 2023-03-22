/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.common.lucene.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

import java.util.Arrays;

/**
 * Extension of the TopDocs for the multiple sub-queries
 */
public class CompoundQueryTopDocs extends TopDocs {

    TopDocs[] docs;

    public CompoundQueryTopDocs(TotalHits totalHits, ScoreDoc[] scoreDocs) {
        super(totalHits, scoreDocs);
    }

    public CompoundQueryTopDocs(TotalHits totalHits, TopDocs[] docs) {
        super(totalHits, copyScoreDocs(docs[0].scoreDocs));
        this.docs = docs;
    }

    private static ScoreDoc[] copyScoreDocs(ScoreDoc[] original) {
        if (original == null) {
            return null;
        }
        // do deep copy
        ScoreDoc[] copy = new ScoreDoc[original.length];
        for (int i = 0; i < original.length; i++) {
            ScoreDoc oneOriginalDoc = original[i];
            copy[i] = new ScoreDoc(oneOriginalDoc.doc, oneOriginalDoc.score, oneOriginalDoc.shardIndex);
        }
        return copy;
    }

    public TopDocs[] getDocs() {
        return docs;
    }

    @Override
    public String toString() {
        StringBuilder docsSb = new StringBuilder();
        if (docs == null || docs.length == 0) {
            docsSb.append("[]");
        } else {
            docsSb.append("\n");
            for (TopDocs td : docs) {
                docsSb.append("[")
                    .append(" totalHits=")
                    .append(td.totalHits)
                    .append(", scoreDocs=")
                    .append(Arrays.toString(td.scoreDocs))
                    .append("],\n");
            }
        }
        return "CompoundQueryTopDocs {"
            + "\ntotalHits="
            + totalHits.toString()
            + "\nscoreDocs="
            + Arrays.toString(scoreDocs)
            + "\ndocs="
            + docsSb
            + '}';
    }
}
